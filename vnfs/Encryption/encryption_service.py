#!/usr/bin/env python3
"""
Encryption VNF - High-Performance Version
"""

import socket
import threading
import time
import logging
import requests
from collections import deque
from cryptography.hazmat.primitives.ciphers import Cipher, algorithms, modes
from cryptography.hazmat.primitives import padding
from cryptography.hazmat.backends import default_backend
from scapy.all import Ether, IP, Raw, TCP, UDP
from vnf_memory import VNFMemoryTracker

# ============================================================
# LOGGING
# ============================================================
logging.basicConfig(
    level=logging.DEBUG,  # change to INFO after debugging
    format="%(asctime)s [ENC] %(levelname)s: %(message)s"
)

_mem = VNFMemoryTracker("encryption")

# ============================================================
# CONFIGURATION
# ============================================================
UDP_LISTEN_PORT = 30214          # port DPI sends to
TARGET_IP = "158.37.63.80"       # next VNF (comp)
TARGET_PORT = 30903
KPI_SERVER = "http://158.37.63.223:5003/kpi_encryption"
KPI_INTERVAL = 1                 # seconds

# AES key and nonce (keep your existing values)
key = b'@\xdc\x9cN`\x01\xa7\x8d\xa3\xa4\xdcF\xb1\xe6\x01,\xc4p\xff\x1f\x9a&L\xdbFG3\xc5}\xf5F\xcc'
nonce = b'\xf4\x94Q\xf8\xae?\rM\xa2\xbe\xcc.'

# ============================================================
# BATCH SENDING
# ============================================================
send_queue = deque()
send_queue_lock = threading.Lock()
BATCH_SIZE = 100
BATCH_TIMEOUT = 0.010          # 10 ms

sock_out = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
sock_out.setsockopt(socket.SOL_SOCKET, socket.SO_SNDBUF, 16 * 1024 * 1024)

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
        logging.debug(f"Flushed {len(packets)} packets to {TARGET_IP}:{TARGET_PORT}")
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
# ENCRYPTION
# ============================================================
def encrypt_data(data: bytes):
    padder = padding.PKCS7(algorithms.AES.block_size).padder()
    padded_data = padder.update(data) + padder.finalize()
    cipher = Cipher(algorithms.AES(key), modes.GCM(nonce), backend=default_backend())
    encryptor = cipher.encryptor()
    encrypted = encryptor.update(padded_data) + encryptor.finalize()
    return encrypted, encryptor.tag

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
# PACKET PROCESSING (robust parser)
# ============================================================
def process_payload(raw_packet_bytes: bytes):
    global traffic_bytes, processing_times

    # Attempt to parse as Ethernet frame first
    packet = None
    try:
        packet = Ether(raw_packet_bytes)
    except Exception:
        pass

    # Fallback to direct IP parsing
    if packet is None or IP not in packet:
        try:
            packet = IP(raw_packet_bytes)
        except Exception as e:
            logging.debug(f"Both Ether and IP parsing failed: {e}")
            return

    if IP not in packet:
        return

    ip_layer = packet[IP]

    # Only accept packets from DPI host
    # if ip_layer.src != '158.37.63.106':
    #     return

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

    # Extract payload and encrypt
    data = packet[Raw].load
    encrypted_data, tag = encrypt_data(data)

    # Forward the plaintext (or encrypted_data) to next VNF
    batch_send_to_target(data)   # change to encrypted_data if desired

    process_time = time.time() - process_start
    with lock:
        processing_times.append(process_time)

# ============================================================
# UDP RECEIVER (high‑performance)
# ============================================================
def udp_receiver():
    sock_in = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    sock_in.setsockopt(socket.SOL_SOCKET, socket.SO_RCVBUF, 16 * 1024 * 1024)
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
            process_payload(data)
        except Exception as e:
            logging.error(f"UDP receive error: {e}")

# ============================================================
# MAIN
# ============================================================
if __name__ == "__main__":
    send_kpi_data()
    threading.Thread(target=batch_sender_thread, daemon=True).start()
    udp_receiver()