#!/usr/bin/env python3
"""
Compression VNF – High‑Performance with Multi‑Threaded Workers
"""

import socket
import threading
import time
import logging
import requests
import brotli
import base64
from collections import deque
from queue import Queue
from scapy.all import Ether, IP, Raw, TCP, UDP
from vnf_memory import VNFMemoryTracker

# ============================================================
# LOGGING
# ============================================================
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [COMP] %(levelname)s: %(message)s"
)

_mem = VNFMemoryTracker("compression")

# ============================================================
# CONFIGURATION
# ============================================================
UDP_LISTEN_PORT = 30903
TARGET_IP = "158.39.48.233"
TARGET_PORT = 30278
KPI_SERVER = "http://158.37.63.223:5004/kpi_compression"
KPI_INTERVAL = 1

WORKER_THREADS = 4          # Increase to number of CPU cores
QUEUE_SIZE = 20000
BATCH_SIZE = 256
BATCH_TIMEOUT = 0.002

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
# COMPRESSION
# ============================================================
# def compress_data(data: bytes) -> bytes:
#     compressed = brotli.compress(data)
#     return base64.b64encode(compressed)

import lz4.frame

def compress_data(data: bytes) -> bytes:
    return lz4.frame.compress(data, compression_level=0)  # fastest

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

    # Parse packet
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

    if not packet.haslayer(Raw):
        return

    # Flow key for memory tracking
    orig_sport = None
    dst_port = None
    if packet.haslayer(TCP):
        orig_sport = packet[TCP].sport
        dst_port = packet[TCP].dport
    elif packet.haslayer(UDP):
        orig_sport = packet[UDP].sport
        dst_port = packet[UDP].dport
    flow_key = (ip_layer.proto, ip_layer.src, orig_sport, ip_layer.dst, dst_port)
    _mem.record_packet(raw_packet_bytes, flow_key)

    process_start = time.time()

    # Count incoming bytes
    with lock:
        traffic_bytes += len(raw_packet_bytes)

    # Extract payload and compress
    data = packet[Raw].load
    compressed_data = compress_data(data)

    # Forward
    batch_send_to_target(compressed_data)

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
            packet_count += 1
            if time.time() - last_log >= 1.0:
                logging.info(f"Received {packet_count} packets in last second")
                packet_count = 0
                last_log = time.time()

            # Non‑blocking put – drop if queue full (should not happen with large queue)
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
    workers = []
    for _ in range(WORKER_THREADS):
        t = threading.Thread(target=worker, args=(packet_queue,), daemon=True)
        t.start()
        workers.append(t)

    # Start receiver (blocking)
    udp_receiver(packet_queue)