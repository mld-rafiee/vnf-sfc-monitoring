# vnf_memory.py  — drop-in memory-scaling module for all VNFs
import ctypes
import gc
import threading
import time
from collections import deque

# ── malloc_trim: forces Python to return freed memory to OS ──────────────────
try:
    _libc = ctypes.CDLL("libc.so.6")
    def release_memory_to_os():
        gc.collect()
        _libc.malloc_trim(0)
except Exception:
    def release_memory_to_os():
        gc.collect()

# ── Tunable parameters ────────────────────────────────────────────────────────
WINDOW_SECONDS           = 30   # sliding window length — main memory lever
PAYLOAD_HISTORY_PER_FLOW = 100  # raw payloads stored per flow
FLOW_EXPIRY_SECONDS      = 5    # evict idle flows after this long
CLEANUP_INTERVAL         = 2    # how often cleanup runs (seconds)

# ── Per-VNF state (one instance per VNF process) ─────────────────────────────
class VNFMemoryTracker:
    def __init__(self, name="vnf"):
        self.name = name

        # Connection table: flow_key → {count, last_seen, recent_payloads}
        self._conn_table      = {}
        self._conn_table_lock = threading.Lock()

        # Sliding window packet log: deque of (timestamp, raw_bytes)
        self._packet_log      = deque()
        self._packet_log_lock = threading.Lock()

        # Start background cleanup
        t = threading.Thread(target=self._cleanup_loop, daemon=True)
        t.start()

    def record_packet(self, pkt_bytes: bytes, flow_key: tuple):
        """
        Call this for every packet the VNF processes.
        pkt_bytes : raw bytes of the packet
        flow_key  : any hashable tuple identifying the flow
                    e.g. (proto, src_ip, src_port, dst_ip, dst_port)
        """
        now = time.time()

        # 1. Update connection table
        with self._conn_table_lock:
            if flow_key not in self._conn_table:
                self._conn_table[flow_key] = {
                    "count":           0,
                    "first_seen":      now,
                    "last_seen":       now,
                    "recent_payloads": deque(maxlen=PAYLOAD_HISTORY_PER_FLOW),
                }
            entry = self._conn_table[flow_key]
            entry["count"]    += 1
            entry["last_seen"] = now
            entry["recent_payloads"].append(pkt_bytes)   # ~1500 bytes each

        # 2. Append to sliding window log
        with self._packet_log_lock:
            self._packet_log.append((now, pkt_bytes))

    def stats(self) -> dict:
        """Return current memory-relevant stats (useful for KPI endpoint)."""
        with self._conn_table_lock:
            flows = len(self._conn_table)
        with self._packet_log_lock:
            log_bytes = sum(len(e[1]) for e in self._packet_log)
        return {"active_flows": flows, "packet_log_bytes": log_bytes}

    # ── Background cleanup ────────────────────────────────────────────────────
    def _cleanup_loop(self):
        while True:
            time.sleep(CLEANUP_INTERVAL)
            now = time.time()

            # Evict expired flows
            evicted = 0
            with self._conn_table_lock:
                expired = [k for k, v in self._conn_table.items()
                           if now - v["last_seen"] > FLOW_EXPIRY_SECONDS]
                for k in expired:
                    del self._conn_table[k]
                evicted = len(expired)

            # Trim packet log to window
            trimmed = 0
            cutoff = now - WINDOW_SECONDS
            with self._packet_log_lock:
                while self._packet_log and self._packet_log[0][0] < cutoff:
                    self._packet_log.popleft()
                    trimmed += 1

            # Return freed pages to OS — this is what makes kubectl top drop
            if evicted > 0 or trimmed > 0:
                release_memory_to_os()