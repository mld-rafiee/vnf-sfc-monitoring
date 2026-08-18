#!/usr/bin/env python3
"""
High‑performance DPI VNF using AF_PACKET raw sockets.
Replaces Scapy capture – now forwards at 30+ Mbps without drops.
"""

import subprocess
import logging
import threading
import queue
import time
import socket
import struct
import requests
from collections import deque

from vnf_memory import VNFMemoryTracker

_mem = VNFMemoryTracker("dpi")

############################################################
# CONFIGURATION
############################################################

INTERFACE = "enp1s0"

# src_ip → (dst_ip, dst_port)
SFC_TARGETS = {
    "158.37.63.110": ("158.37.63.132", 30214),   # SFC 1
    "158.39.201.62": ("158.37.65.40",  30214),   # SFC 2
}

SNORT_CONFIG = "/etc/snort/snort.conf"
KPI_SERVER   = "http://158.37.63.223:5002/kpi_dpi"
KPI_INTERVAL = 1

WORKER_THREADS = 4
QUEUE_SIZE     = 20000        # increased for burst absorption

batch_size    = 256           # larger batches improve throughput
batch_timeout = 0.001         # 1 ms flush interval

############################################################
# LOGGING
############################################################

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [DPI] %(levelname)s: %(message)s"
)

############################################################
# GLOBAL STATE
############################################################

processing_times = []
traffic_bytes    = 0
start_time       = time.time()
lock             = threading.Lock()

############################################################
# PER‑SFC UDP SOCKETS AND SEND QUEUES
############################################################

# One socket per destination IP
sockets = {
    dst_ip: socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    for dst_ip, _ in SFC_TARGETS.values()
}

# One deque per destination IP (holds raw bytes)
send_queues = {
    dst_ip: deque()
    for dst_ip, _ in SFC_TARGETS.values()
}

send_queue_lock = threading.Lock()

############################################################
# BATCH SEND (RAW BYTES)
############################################################

def batch_send_to_target(raw_packet: bytes, dst_ip: str, dst_port: int):
    """Add raw packet to the correct SFC queue; flush if batch full."""
    with send_queue_lock:
        send_queues[dst_ip].append(raw_packet)
        if len(send_queues[dst_ip]) >= batch_size:
            _flush_locked(dst_ip, dst_port)

def _flush_locked(dst_ip: str, dst_port: int):
    """Flush one queue — must be called while holding send_queue_lock."""
    try:
        packets_to_send = list(send_queues[dst_ip])
        send_queues[dst_ip].clear()
        for pkt in packets_to_send:
            sockets[dst_ip].sendto(pkt, (dst_ip, dst_port))
    except Exception as e:
        logging.error(f"Batch send error to {dst_ip}:{dst_port} — {e}")

def flush_all_batches():
    """Timeout flush — drain all queues."""
    with send_queue_lock:
        for src_ip, (dst_ip, dst_port) in SFC_TARGETS.items():
            if send_queues[dst_ip]:
                _flush_locked(dst_ip, dst_port)

def batch_sender_thread():
    """Periodically flush batches on timeout."""
    while True:
        time.sleep(batch_timeout)
        flush_all_batches()

############################################################
# PACKET QUEUE (holds (raw_bytes, src_ip))
############################################################

packet_queue = queue.Queue(maxsize=QUEUE_SIZE)

############################################################
# SNORT
############################################################

def start_snort():
    logging.info("Starting Snort IDS")
    proc = subprocess.Popen(
        [
            "snort",
            "-i", INTERFACE,
            "-c", SNORT_CONFIG,
            "-A", "fast",
            "-D"
        ],
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        text=True,
        bufsize=1
    )
    time.sleep(2)
    if proc.poll() is not None:
        err = proc.stderr.read()
        logging.error(f"Snort failed to start:\n{err}")
        raise RuntimeError("Snort exited immediately — check config")
    logging.info(f"Snort started (pid={proc.pid})")
    return proc

def read_snort_alerts(proc):
    """Read stdout and stderr from Snort in separate threads."""
    def _read(stream, label):
        for line in stream:
            if line.strip():
                logging.info(f"SNORT {label}: {line.strip()}")
    threading.Thread(target=_read, args=(proc.stdout, "ALERT"), daemon=True).start()
    threading.Thread(target=_read, args=(proc.stderr, "ERROR"), daemon=True).start()

############################################################
# AF_PACKET CAPTURE (HIGH‑PERFORMANCE)
############################################################

def extract_src_ip(raw_packet: bytes):
    """
    Extract source IPv4 address from raw Ethernet frame.
    Handles VLAN tags (802.1Q). Returns dotted string or None.
    """
    eth_len = 14
    if len(raw_packet) < eth_len:
        return None

    # Ethernet type field
    ethertype = struct.unpack('!H', raw_packet[12:14])[0]

    # Handle VLAN tag (0x8100)
    if ethertype == 0x8100:
        if len(raw_packet) < 18:
            return None
        ethertype = struct.unpack('!H', raw_packet[16:18])[0]
        ip_start = 18
    else:
        ip_start = eth_len

    # Only IPv4 (0x0800)
    if ethertype != 0x0800:
        return None

    if len(raw_packet) < ip_start + 20:
        return None

    # IP header: source IP at offset 12
    src_ip_bytes = raw_packet[ip_start + 12 : ip_start + 16]
    return socket.inet_ntoa(src_ip_bytes)

def af_packet_capture():
    """Main capture loop using AF_PACKET raw socket."""
    # Create raw socket (ETH_P_ALL)
    sock = socket.socket(socket.AF_PACKET, socket.SOCK_RAW, socket.htons(3))
    sock.bind((INTERFACE, 0))

    # Increase socket receive buffer (16 MB) to avoid kernel drops
    sock.setsockopt(socket.SOL_SOCKET, socket.SO_RCVBUF, 16 * 1024 * 1024)

    logging.info(f"AF_PACKET capture started on {INTERFACE}")

    while True:
        try:
            raw_data = sock.recv(65535)
        except Exception as e:
            logging.error(f"recv error: {e}")
            continue

        # Quick software filter: extract source IP
        src_ip = extract_src_ip(raw_data)
        if src_ip is None:
            continue
        if src_ip not in SFC_TARGETS:
            continue

        # Push to worker queue (non‑blocking)
        try:
            packet_queue.put_nowait((raw_data, src_ip))
        except queue.Full:
            logging.warning("Packet queue full — dropping packet")

############################################################
# DPI WORKER
############################################################

def dpi_worker():
    global traffic_bytes
    while True:
        raw_data, src_ip = packet_queue.get()
        start_proc = time.time()

        try:
            dst_ip, dst_port = SFC_TARGETS[src_ip]

            # Memory tracking (raw bytes)
            _mem.record_packet(bytes(raw_data), (src_ip, dst_ip, dst_port))

            # Forward to next VNF
            batch_send_to_target(raw_data, dst_ip, dst_port)

            with lock:
                traffic_bytes += len(raw_data)

        except Exception as e:
            logging.error(f"Processing error: {e}")

        finally:
            proc_delay = time.time() - start_proc
            with lock:
                processing_times.append(proc_delay)
            packet_queue.task_done()

############################################################
# KPI REPORTING
############################################################

def send_kpi_data():
    global processing_times, traffic_bytes, start_time
    with lock:
        elapsed      = time.time() - start_time
        avg_delay    = sum(processing_times) / len(processing_times) if processing_times else 0.0
        traffic_rate = (traffic_bytes * 8) / elapsed / (1024 * 1024) if elapsed > 0 else 0.0
        processing_times.clear()
        traffic_bytes = 0
        start_time    = time.time()

    try:
        requests.post(
            KPI_SERVER,
            json={
                "processing_delay": avg_delay,
                "traffic_rate_mbps": traffic_rate
            },
            timeout=2
        )
    except requests.RequestException:
        logging.warning("KPI server unreachable")

def kpi_loop():
    while True:
        time.sleep(KPI_INTERVAL)
        send_kpi_data()

############################################################
# MAIN
############################################################

def main():
    # Start Snort
    snort_proc = start_snort()
    read_snort_alerts(snort_proc)

    # Batch sender thread
    threading.Thread(target=batch_sender_thread, daemon=True).start()

    # DPI worker threads
    for _ in range(WORKER_THREADS):
        threading.Thread(target=dpi_worker, daemon=True).start()

    # KPI reporter thread
    threading.Thread(target=kpi_loop, daemon=True).start()

    # Blocking capture – runs forever
    af_packet_capture()

if __name__ == "__main__":
    main()