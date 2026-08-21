import json
import threading
import requests
import queue
import socket
import time
from scapy.all import sniff, IP, TCP, UDP
from vnf_queue import VNFMemoryTracker
from concurrent.futures import ThreadPoolExecutor
from collections import deque

_mem = VNFMemoryTracker("firewall")
send_queue = deque()
send_queue_lock = threading.Lock()
batch_size = 100
batch_timeout = 0.010  # 1ms



target_ip = "158.37.63.106"   # worker2   158.39.48.233   158.37.63.106
target_port = 30278 # worker2
duration = 1  # one second duration



class FirewallRule:
    def __init__(self, protocol, src_ip, src_port, dst_ip, dst_port, action):
        self.protocol = protocol
        self.src_ip = src_ip
        self.src_port = src_port
        self.dst_ip = dst_ip
        self.dst_port = dst_port
        self.action = action



def load_rules(filename):
    with open(filename, 'r') as f:
        rules_json = json.load(f)
        rules_dict = {}

        for rule in rules_json:
            # Convert the protocol to a number (17 for UDP, 6 for TCP)
            protocol = 17 if rule['protocol'].lower() == 'udp' else 6 if rule['protocol'].lower() == 'tcp' else None
            src_ip = rule['src_ip']
            src_port = rule['src_port']
            dst_ip = rule['dst_ip']
            dst_port = rule['dst_port']
            action = rule['action']

            # Use a tuple as the dictionary key
            rule_key = (protocol, src_ip, src_port, dst_ip, dst_port)
            rules_dict[rule_key] = action
        
        return rules_dict

rules = load_rules("firewall_rules.json")

processing_times = []
queuing_delays = []
start_time = time.time()

traffic_bytes = 0
lock = threading.Lock()

def send_kpi_data():
    """Send KPI data every second"""
    global processing_times, traffic_bytes, start_time
    
    with lock:
        elapsed = time.time() - start_time
        
        # Calculate metrics
        average_processing_delay = sum(processing_times) / len(processing_times) if processing_times else 0.0
        traffic_rate_mbps = ((traffic_bytes * 8) / elapsed / (1024 * 1024)) if elapsed > 0 else 0.0
        
        try:
            response = requests.post(
                "http://158.37.63.223:5007/kpi_firewall2",
                json={
                    "processing_delay": average_processing_delay,
                    "traffic_rate_mbps": traffic_rate_mbps
                },
                timeout=2  # 2 second timeout
            )
            response.raise_for_status()
            #print(f"Sent KPI - Traffic: {traffic_rate_mbps:.2f} MB/s, Processing Delay: {average_processing_delay:.6f}s")
        except requests.exceptions.Timeout:
            print(f"⚠ KPI send timeout")
        except requests.exceptions.ConnectionError:
            print(f"⚠ KPI server not reachable")
        except requests.RequestException as e:
            print(f"⚠ Failed to send KPI: {type(e).__name__}")
        
        # Reset for the next measurement window
        processing_times.clear()
        traffic_bytes = 0
        start_time = time.time()
    
    # Schedule next execution
    threading.Timer(duration, send_kpi_data).start()

def process_packet(packet):
    global processing_times, traffic_bytes

    if IP not in packet:
        return
    
    ip_layer = packet[IP]
    if ip_layer.src != '158.39.48.233':
        return

    
    process_start_time = time.time()

    # Count bytes and packets for traffic rate calculation
    packet_size = len(bytes(packet))  # Total packet size in bytes
    
    with lock:
        traffic_bytes += packet_size
    

    protocol = ip_layer.proto
    src_ip = ip_layer.src
    src_port = None
    dst_ip = ip_layer.dst
    dst_port = None

    # Check if TCP or UDP
    if protocol == 6 and TCP in packet:  # TCP
        src_port = packet[TCP].sport
        dst_port = packet[TCP].dport
    elif protocol == 17 and UDP in packet:  # UDP
        src_port = packet[UDP].sport
        dst_port = packet[UDP].dport

    
    # Lookup the rule based on protocol, src_ip, src_port, dst_ip, dst_port
    rule_key = (protocol, src_ip, src_port, dst_ip, dst_port)
    action = rules.get(rule_key, 'DENY')  # Default action is DENY if no match found

    # if action == 'ALLOW':
    #     print("allow")

    flow_key = (protocol, src_ip, src_port, dst_ip, dst_port)
    _mem.record_packet(bytes(packet), flow_key)
    
    try:
        #requests.post(server_url, json={"response": response})
        # send_to_target(packet)
        batch_send_to_target(packet)
        #print("hello")
    except requests.RequestException as e:
        print(f"Failed to send response: {e}")

    #print(f"Received packet: {response}")
    process_end_time = time.time()

    with lock:
        processing_times.append(process_end_time - process_start_time)

    


sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)

def send_to_target(packet):
    try:
        packet_bytes = bytes(packet)
        sock.sendto(packet_bytes, (target_ip, target_port))
        #print("packet is sent")

    except Exception as e:
        print(f"Error sending packet to client: {e}")


def batch_send_to_target(packet):
    """Add packet to batch queue"""
    global send_queue
    with send_queue_lock:
        send_queue.append(bytes(packet))
        
        if len(send_queue) >= batch_size:
            flush_batch_locked()

def flush_batch_locked():
    """Send all packets in batch (call only while holding send_queue_lock)"""
    try:
        # Create a copy and clear atomically
        packets_to_send = list(send_queue)
        send_queue.clear()
        
        # Send outside the lock to avoid blocking other threads
        for packet_bytes in packets_to_send:
            sock.sendto(packet_bytes, (target_ip, target_port))
    except Exception as e:
        print(f"Error in batch send: {e}")

def flush_batch():
    """Send all packets in batch (thread-safe wrapper)"""
    with send_queue_lock:
        if send_queue:
            flush_batch_locked()

def batch_sender_thread():
    """Periodically flush batch if timeout"""
    while True:
        time.sleep(batch_timeout)
        flush_batch()



if __name__ == "__main__":
    send_kpi_data()

    threading.Thread(target=batch_sender_thread, daemon=True).start()

    sniff(iface="enp1s0", filter="ip src 158.39.48.233", prn=process_packet, store=0)
