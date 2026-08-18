#!/usr/bin/env python3
"""
NAT VNF - High-Performance with Multi-Threaded Workers
"""

import socket
import threading
import time
import logging
import requests
from collections import deque
from queue import Queue
from scapy.all import Ether, IP, TCP, UDP
from NAT.vnf_queue import VNFMemoryTracker

# ============================================================
# LOGGING
# ============================================================
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [NAT] %(levelname)s: %(message)s"
)

_mem = VNFMemoryTracker("nat")

# ============================================================
# CONFIGURATION
# ============================================================
UDP_LISTEN_PORT = 30214          # port DPI sends to for nat branch
NAT_PUBLIC_IP   = "158.37.63.81"
TARGET_IP       = "158.39.48.233"
TARGET_PORT     = 30278
KPI_SERVER      = "http://158.37.63.223:5008/kpi_nat"
KPI_INTERVAL    = 1

PORT_POOL_START = 10000
PORT_POOL_END   = 60000

WORKER_THREADS  = 4
QUEUE_SIZE      = 20000
BATCH_SIZE      = 256
BATCH_TIMEOUT   = 0.002

# ============================================================
# NAT STATE
# ============================================================
nat_table      = {}
nat_table_rev  = {}
next_port      = PORT_POOL_START
nat_table_lock = threading.Lock()

def allocate_nat_port(orig_ip: str, orig_port: int, proto: int) -> int:
    global next_port
    key = (orig_ip, orig_port, proto)
    with nat_table_lock:
        if key in nat_table:
            return nat_table[key]
        port = next_port
        next_port += 1
        if next_port > PORT_POOL_END:
            next_port = PORT_POOL_START
        nat_table[key] = port
        nat_table_rev[(port, proto)] = (orig_ip, orig_port)
        return port

# ============================================================
# BATCH SENDING (shared by all workers)
# ============================================================
send_queue = deque()
send_queue_lock = threading.Lock()
sock_out = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
sock_out.setsockopt(socket.SOL_SOCKET, socket.SO_SNDBUF, 32 * 1024 * 1024)

def batch_send_to_target(data: bytes):
    with send_queue_lock:
        send_queue.append(data)
        if len(send_queue) >= BATCH_SIZE:
            _flush_locked()

def _flush_locked():
    try:
        packets = list(send_queue)
        send_queue.clear()
        for pkt in packets:
            sock_out.sendto(pkt, (TARGET_IP, TARGET_PORT))
        logging.debug(f"Flushed {len(packets)} packets")
    except Exception as e:
        logging.error(f"Batch send error: {e}")

def flush_batch():
    with send_queue_lock:
        if send_queue:
            _flush_locked()

def batch_sender_thread():
    while True:
        time.sleep(BATCH_TIMEOUT)
        flush_batch()

# ============================================================
# KPI METRICS
# ============================================================
processing_times = []
traffic_bytes = 0
start_time = time.time()
lock = threading.Lock()

def send_kpi_data():
    global processing_times, traffic_bytes, start_time
    with lock:
        elapsed = time.time() - start_time
        avg_delay = sum(processing_times) / len(processing_times) if processing_times else 0.0
        traffic_rate = (traffic_bytes * 8) / elapsed / (1024 * 1024) if elapsed > 0 else 0.0
        processing_times.clear()
        traffic_bytes = 0
        start_time = time.time()

    logging.info(f"KPI: delay={avg_delay:.6f}s, rate={traffic_rate:.2f} Mbps")
    try:
        requests.post(KPI_SERVER, json={
            "processing_delay": avg_delay,
            "traffic_rate_mbps": traffic_rate
        }, timeout=2)
    except Exception as e:
        logging.warning(f"KPI send failed: {e}")

    threading.Timer(KPI_INTERVAL, send_kpi_data).start()

# ============================================================
# PACKET PROCESSING (called by workers)
# ============================================================
def process_payload(raw_packet_bytes: bytes):
    global traffic_bytes, processing_times

    # Robust parsing: try Ethernet then IP
    packet = None
    try:
        packet = Ether(raw_packet_bytes)
    except:
        pass
    if packet is None or IP not in packet:
        try:
            packet = IP(raw_packet_bytes)
        except:
            return

    if IP not in packet:
        return

    ip_layer = packet[IP]

    # Memory tracking
    orig_sport = None
    dst_port = None
    proto = ip_layer.proto
    if proto == 6 and packet.haslayer(TCP):
        orig_sport = packet[TCP].sport
        dst_port   = packet[TCP].dport
    elif proto == 17 and packet.haslayer(UDP):
        orig_sport = packet[UDP].sport
        dst_port   = packet[UDP].dport
    flow_key = (proto, ip_layer.src, orig_sport, ip_layer.dst, dst_port)
    _mem.record_packet(raw_packet_bytes, flow_key)

    process_start = time.time()

    # Count incoming bytes
    with lock:
        traffic_bytes += len(raw_packet_bytes)

    # Copy and apply NAT
    new_pkt = packet.copy()
    new_pkt[IP].src = NAT_PUBLIC_IP

    if proto == 6 and new_pkt.haslayer(TCP):
        nat_port = allocate_nat_port(ip_layer.src, orig_sport, proto)
        new_pkt[TCP].sport = nat_port
        del new_pkt[IP].chksum
        del new_pkt[TCP].chksum
    elif proto == 17 and new_pkt.haslayer(UDP):
        nat_port = allocate_nat_port(ip_layer.src, orig_sport, proto)
        new_pkt[UDP].sport = nat_port
        del new_pkt[IP].chksum
        del new_pkt[UDP].chksum
    else:
        del new_pkt[IP].chksum

    # Forward modified packet
    batch_send_to_target(bytes(new_pkt))

    process_time = time.time() - process_start
    with lock:
        processing_times.append(process_time)

# ============================================================
# WORKER THREAD
# ============================================================
def worker(packet_queue: Queue):
    while True:
        raw_data = packet_queue.get()
        if raw_data is None:   # poison pill
            break
        process_payload(raw_data)
        packet_queue.task_done()

# ============================================================
# UDP RECEIVER (Producer)
# ============================================================
def udp_receiver(packet_queue: Queue):
    sock_in = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    sock_in.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
    sock_in.setsockopt(socket.SOL_SOCKET, socket.SO_RCVBUF, 32 * 1024 * 1024)
    sock_in.bind(("0.0.0.0", UDP_LISTEN_PORT))
    logging.info(f"Listening on UDP port {UDP_LISTEN_PORT}")

    packet_count = 0
    last_log = time.time()

    while True:
        try:
            data, addr = sock_in.recvfrom(65535)
            # Only accept from DPI host
            if addr[0] != "158.37.63.106":
                continue

            packet_count += 1
            if time.time() - last_log >= 1.0:
                logging.info(f"Received {packet_count} packets in last second")
                packet_count = 0
                last_log = time.time()

            try:
                packet_queue.put_nowait(data)
            except:
                logging.warning("Packet queue full – dropping packet")
        except Exception as e:
            logging.error(f"UDP receive error: {e}")

# ============================================================
# MAIN
# ============================================================
if __name__ == "__main__":
    send_kpi_data()
    threading.Thread(target=batch_sender_thread, daemon=True).start()

    packet_queue = Queue(maxsize=QUEUE_SIZE)

    # Start worker threads
    for _ in range(WORKER_THREADS):
        t = threading.Thread(target=worker, args=(packet_queue,), daemon=True)
        t.start()

    # Start receiver (blocking)
    udp_receiver(packet_queue)