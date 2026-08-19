from flask import Flask, request, jsonify
import sqlite3
from kubernetes import client, config
import subprocess
import time
import concurrent.futures
import threading
from datetime import datetime, timedelta

from collections import deque

# Configuration
namespace = 'default'
interval = 1
TIMEOUT_SECONDS = 5  # Consider VNF crashed if no update in 5 seconds

app = Flask(__name__)

class CrashLogger:
    def __init__(self):
        self.crash_history = {
            'firewall-app': deque(maxlen=5),
            'dpi-app': deque(maxlen=5),
            'enc-app': deque(maxlen=5),
            'comp-app': deque(maxlen=5),
            'firewall2-app': deque(maxlen=5),
            'nat-app': deque(maxlen=5),
        }
        self.episode_crash_count = {app: 0 for app in self.crash_history}  # Track crashes per episode


    def log_crash(self, app_name, value):
        """Logs a crash count for a given application."""
        if app_name in self.crash_history:
            self.crash_history[app_name].append(value)
            self.episode_crash_count[app_name] += value  # Track crashes per episode
        else:
            print(f"Unknown app: {app_name}")

    def get_history(self, app_name):
        """Returns the last 10 crash values for the given application."""
        return list(self.crash_history.get(app_name, []))

    def get_count(self, app_name):
        """Returns the total sum of crashes recorded in the last 10 entries for the given application."""
        return sum(self.crash_history.get(app_name, []))
    
    def get_last_value(self, app_name):
        """Returns the last recorded crash value for the given application. If none, returns 0."""
        return self.crash_history[app_name][-1] if self.crash_history[app_name] else 0

    def reset_episode_crashes(self):
        """Reset crash count for a new episode."""
        self.episode_crash_count = {app: 0 for app in self.crash_history}

    def get_episode_crash_count(self):
        """Returns the number of crashes per app for the current episode."""
        return self.episode_crash_count
    
# crash_logger_global = None

def update_database(key, value):
    """Update database with timestamp for timeout detection"""
    conn = sqlite3.connect('shared_data_temp_top2.db', check_same_thread=False, timeout=10)
    cursor = conn.cursor()
    try:
        cursor.execute('CREATE TABLE IF NOT EXISTS data (key TEXT PRIMARY KEY, value TEXT, timestamp REAL)')
        # Store current timestamp along with value
        cursor.execute('INSERT OR REPLACE INTO data (key, value, timestamp) VALUES (?, ?, ?)', 
                      (key, value, time.time()))
        conn.commit()
    except sqlite3.OperationalError as e:
        print(f"Database error: {e}")
    finally:
        cursor.close()
        conn.close()

@app.route('/kpi_firewall', methods=['POST'])
def firewall():
    vnf_name = "firewall"
    processing_delay = request.json.get('processing_delay')
    traffic_rate = request.json.get('traffic_rate_mbps')
    update_database(f'processing_delay_{vnf_name}', processing_delay)
    update_database(f'traffic_rate_{vnf_name}', traffic_rate)
    print(f"Received {vnf_name} processing delay: {processing_delay}, traffic rate: {traffic_rate}")
    return jsonify({"status": "success"}), 200

@app.route('/kpi_dpi', methods=['POST'])
def dpi():
    vnf_name = "dpi"
    processing_delay = request.json.get('processing_delay')
    traffic_rate = request.json.get('traffic_rate_mbps')
    update_database(f'processing_delay_{vnf_name}', processing_delay)
    update_database(f'traffic_rate_{vnf_name}', traffic_rate)
    print(f"Received {vnf_name} processing delay: {processing_delay}, traffic rate: {traffic_rate}")
    return jsonify({"status": "success"}), 200

@app.route('/kpi_encryption', methods=['POST'])
def encryption():
    vnf_name = "encryption"
    processing_delay = request.json.get('processing_delay')
    traffic_rate = request.json.get('traffic_rate_mbps')
    update_database(f'processing_delay_{vnf_name}', processing_delay)
    update_database(f'traffic_rate_{vnf_name}', traffic_rate)
    print(f"Received {vnf_name} processing delay: {processing_delay}, traffic rate: {traffic_rate}")
    return jsonify({"status": "success"}), 200

@app.route('/kpi_compression', methods=['POST'])
def compression():
    vnf_name = "compression"
    processing_delay = request.json.get('processing_delay')
    traffic_rate = request.json.get('traffic_rate_mbps')
    update_database(f'processing_delay_{vnf_name}', processing_delay)
    update_database(f'traffic_rate_{vnf_name}', traffic_rate)
    print(f"Received {vnf_name} processing delay: {processing_delay}, traffic rate: {traffic_rate}")
    return jsonify({"status": "success"}), 200

@app.route('/kpi_rtt', methods=['POST'])
def rtt():
    avg_rtt = request.json.get('average_rtt')
    update_database('average_rtt', avg_rtt)
    print(f"Received average rtt: {avg_rtt}")
    return jsonify({"status": "success"}), 200

@app.route('/kpi_transmission_rate', methods=['POST'])
def transmission_rate():
    transmission_rate = request.json.get('transmission_rate')
    update_database('transmission_rate', transmission_rate)
    print(f"Received transmission rate: {transmission_rate}")
    return jsonify({"status": "success"}), 200

@app.route('/kpi_firewall2', methods=['POST'])
def firewall2():
    vnf_name = "firewall2"
    processing_delay = request.json.get('processing_delay')
    traffic_rate = request.json.get('traffic_rate_mbps')
    update_database(f'processing_delay_{vnf_name}', processing_delay)
    update_database(f'traffic_rate_{vnf_name}', traffic_rate)
    print(f"Received {vnf_name} processing delay: {processing_delay}, traffic rate: {traffic_rate}")
    return jsonify({"status": "success"}), 200

@app.route('/kpi_nat', methods=['POST'])
def nat():
    vnf_name = "nat"
    processing_delay = request.json.get('processing_delay')
    traffic_rate = request.json.get('traffic_rate_mbps')
    update_database(f'processing_delay_{vnf_name}', processing_delay)
    update_database(f'traffic_rate_{vnf_name}', traffic_rate)
    print(f"Received {vnf_name} processing delay: {processing_delay}, traffic rate: {traffic_rate}")
    return jsonify({"status": "success"}), 200


# def get_pod_details_firewall():
#     global crash_logger_global
#     config.load_kube_config(config_file="/home/ubuntu/.kube/config")
#     v1 = client.CoreV1Api()
#     try:
#         result = subprocess.run(["kubectl", "top", "pod", "-l", "app=firewall-app", "--containers"], 
#                               capture_output=True, text=True)

#         if result.returncode != 0 or not result.stdout:
#             stderr_lines = result.stderr.split()
#             pod_name = None
#             for part in stderr_lines:
#                 if 'firewall-app' in part:
#                     pod_name = part.split('/')[-1].rstrip(',')
#                     break

#             pod = v1.read_namespaced_pod(name=pod_name, namespace='default')  
#             cpu_request = pod.spec.containers[0].resources.requests['cpu']
#             memory_request = pod.spec.containers[0].resources.requests['memory']

#             is_crashed_now = crash_logger_global.get_count("firewall-app") 

#             return cpu_request, memory_request, "0", "0", is_crashed_now
        
#         parts = result.stdout.split()
#         pod_name = parts[4]
#         pod = v1.read_namespaced_pod(name=pod_name, namespace='default')  
#         cpu_request = pod.spec.containers[0].resources.requests['cpu']
#         memory_request = pod.spec.containers[0].resources.requests['memory']
#         cpu_usage = parts[6] or "0"
#         memory_usage = parts[7] or "0"

#         is_crashed_now = crash_logger_global.get_count("firewall-app") 

#         return cpu_request, memory_request, cpu_usage, memory_usage, is_crashed_now
#     except Exception as e:
#         print(f"Error retrieving firewall pod details: {e}")
#         is_crashed_now = crash_logger_global.get_count("firewall-app") 
#         return "0", "0", "0", "0", is_crashed_now

# def get_pod_details_dpi():
#     global crash_logger_global
#     config.load_kube_config(config_file="/home/ubuntu/.kube/config")
#     v1 = client.CoreV1Api()
#     try:
#         result = subprocess.run(["kubectl", "top", "pod", "-l", "app=dpi-app", "--containers"], 
#                               capture_output=True, text=True)

#         if result.returncode != 0 or not result.stdout:
#             stderr_lines = result.stderr.split()
#             pod_name = None
#             for part in stderr_lines:
#                 if 'dpi-app' in part:
#                     pod_name = part.split('/')[-1].rstrip(',')
#                     break

#             pod = v1.read_namespaced_pod(name=pod_name, namespace='default')  
#             cpu_request = pod.spec.containers[0].resources.requests['cpu']
#             memory_request = pod.spec.containers[0].resources.requests['memory']
            
#             is_crashed_now = crash_logger_global.get_count("dpi-app") 

#             return cpu_request, memory_request, "0", "0", is_crashed_now
        
#         parts = result.stdout.split()
#         pod_name = parts[4]
#         pod = v1.read_namespaced_pod(name=pod_name, namespace='default')  
#         cpu_request = pod.spec.containers[0].resources.requests['cpu']
#         memory_request = pod.spec.containers[0].resources.requests['memory']
#         cpu_usage = parts[6] or "0"
#         memory_usage = parts[7] or "0"

#         is_crashed_now = crash_logger_global.get_count("dpi-app") 

#         return cpu_request, memory_request, cpu_usage, memory_usage, is_crashed_now
#     except Exception as e:
#         print(f"Error retrieving dpi pod details: {e}")
#         is_crashed_now = crash_logger_global.get_count("dpi-app") 
#         return "0", "0", "0", "0", is_crashed_now

# def get_pod_details_enc():
#     global crash_logger_global
#     config.load_kube_config(config_file="/home/ubuntu/.kube/config")
#     v1 = client.CoreV1Api()
#     try:
#         result = subprocess.run(["kubectl", "top", "pod", "-l", "app=enc-app", "--containers"], 
#                               capture_output=True, text=True)

#         if result.returncode != 0 or not result.stdout:
#             stderr_lines = result.stderr.split()
#             pod_name = None
#             for part in stderr_lines:
#                 if 'enc-app' in part:
#                     pod_name = part.split('/')[-1].rstrip(',')
#                     break

#             pod = v1.read_namespaced_pod(name=pod_name, namespace='default')  
#             cpu_request = pod.spec.containers[0].resources.requests['cpu']
#             memory_request = pod.spec.containers[0].resources.requests['memory']

#             is_crashed_now = crash_logger_global.get_count("enc-app")

#             return cpu_request, memory_request, "0", "0", is_crashed_now

#         parts = result.stdout.split()
#         pod_name = parts[4]
#         pod = v1.read_namespaced_pod(name=pod_name, namespace='default')  
#         cpu_request = pod.spec.containers[0].resources.requests['cpu']
#         memory_request = pod.spec.containers[0].resources.requests['memory']
#         cpu_usage = parts[6] or "0"
#         memory_usage = parts[7] or "0"

#         is_crashed_now = crash_logger_global.get_count("enc-app") 

#         return cpu_request, memory_request, cpu_usage, memory_usage, is_crashed_now
#     except Exception as e:
#         print(f"Error retrieving enc pod details: {e}")
#         is_crashed_now = crash_logger_global.get_count("enc-app")
#         return "0", "0", "0", "0", is_crashed_now

# def get_pod_details_comp():
#     global crash_logger_global
#     config.load_kube_config(config_file="/home/ubuntu/.kube/config")
#     v1 = client.CoreV1Api()
#     try:
#         result = subprocess.run(["kubectl", "top", "pod", "-l", "app=comp-app", "--containers"], 
#                               capture_output=True, text=True)
        
#         if result.returncode != 0 or not result.stdout:
#             stderr_lines = result.stderr.split()
#             pod_name = None
#             for part in stderr_lines:
#                 if 'comp-app' in part:
#                     pod_name = part.split('/')[-1].rstrip(',')
#                     break

#             pod = v1.read_namespaced_pod(name=pod_name, namespace='default')  
#             cpu_request = pod.spec.containers[0].resources.requests['cpu']
#             memory_request = pod.spec.containers[0].resources.requests['memory']

#             is_crashed_now = crash_logger_global.get_count("comp-app")

#             return cpu_request, memory_request, "0", "0", is_crashed_now
        
#         parts = result.stdout.split()
#         pod_name = parts[4]
#         pod = v1.read_namespaced_pod(name=pod_name, namespace='default')  
#         cpu_request = pod.spec.containers[0].resources.requests['cpu']
#         memory_request = pod.spec.containers[0].resources.requests['memory']
#         cpu_usage = parts[6] or "0"
#         memory_usage = parts[7] or "0"

#         is_crashed_now = crash_logger_global.get_count("comp-app") 

#         return cpu_request, memory_request, cpu_usage, memory_usage, is_crashed_now
    
#     except Exception as e:
#         print(f"Error retrieving comp pod details: {e}")
#         is_crashed_now = crash_logger_global.get_count("comp-app") 
#         return "0", "0", "0", "0", is_crashed_now

# def get_pod_details_firewall2():
#     global crash_logger_global
#     config.load_kube_config(config_file="/home/ubuntu/.kube/config")
#     v1 = client.CoreV1Api()
#     try:
#         result = subprocess.run(["kubectl", "top", "pod", "-l", "app=firewall2-app", "--containers"], 
#                               capture_output=True, text=True)

#         if result.returncode != 0 or not result.stdout:
#             stderr_lines = result.stderr.split()
#             pod_name = None
#             for part in stderr_lines:
#                 if 'firewall2-app' in part:
#                     pod_name = part.split('/')[-1].rstrip(',')
#                     break

#             pod = v1.read_namespaced_pod(name=pod_name, namespace='default')  
#             cpu_request = pod.spec.containers[0].resources.requests['cpu']
#             memory_request = pod.spec.containers[0].resources.requests['memory']

#             is_crashed_now = crash_logger_global.get_count("firewall2-app") 

#             return cpu_request, memory_request, "0", "0", is_crashed_now
        
#         parts = result.stdout.split()
#         pod_name = parts[4]
#         pod = v1.read_namespaced_pod(name=pod_name, namespace='default')  
#         cpu_request = pod.spec.containers[0].resources.requests['cpu']
#         memory_request = pod.spec.containers[0].resources.requests['memory']
#         cpu_usage = parts[6] or "0"
#         memory_usage = parts[7] or "0"

#         is_crashed_now = crash_logger_global.get_count("firewall2-app") 

#         return cpu_request, memory_request, cpu_usage, memory_usage, is_crashed_now
#     except Exception as e:
#         print(f"Error retrieving firewall2 pod details: {e}")
#         is_crashed_now = crash_logger_global.get_count("firewall2-app") 
#         return "0", "0", "0", "0", is_crashed_now
    
# def get_pod_details_nat():
#     global crash_logger_global
#     config.load_kube_config(config_file="/home/ubuntu/.kube/config")
#     v1 = client.CoreV1Api()
#     try:
#         result = subprocess.run(["kubectl", "top", "pod", "-l", "app=nat-app", "--containers"], 
#                               capture_output=True, text=True)

#         if result.returncode != 0 or not result.stdout:
#             stderr_lines = result.stderr.split()
#             pod_name = None
#             for part in stderr_lines:
#                 if 'nat-app' in part:
#                     pod_name = part.split('/')[-1].rstrip(',')
#                     break

#             pod = v1.read_namespaced_pod(name=pod_name, namespace='default')  
#             cpu_request = pod.spec.containers[0].resources.requests['cpu']
#             memory_request = pod.spec.containers[0].resources.requests['memory']

#             is_crashed_now = crash_logger_global.get_count("nat-app") 

#             return cpu_request, memory_request, "0", "0", is_crashed_now
        
#         parts = result.stdout.split()
#         pod_name = parts[4]
#         pod = v1.read_namespaced_pod(name=pod_name, namespace='default')  
#         cpu_request = pod.spec.containers[0].resources.requests['cpu']
#         memory_request = pod.spec.containers[0].resources.requests['memory']
#         cpu_usage = parts[6] or "0"
#         memory_usage = parts[7] or "0"

#         is_crashed_now = crash_logger_global.get_count("nat-app") 

#         return cpu_request, memory_request, cpu_usage, memory_usage, is_crashed_now
#     except Exception as e:
#         print(f"Error retrieving nat pod details: {e}")
#         is_crashed_now = crash_logger_global.get_count("nat-app") 
#         return "0", "0", "0", "0", is_crashed_now
    

# Load config once at the top of the file (or in each function)
# config.load_kube_config(config_file="/home/ubuntu/.kube/config")
# v1 = client.CoreV1Api()

def get_pod_details_old(vnf_label: str, vnf_key: str):
    """Generic function to get pod details for any VNF"""
    global crash_logger_global
    
    try:
        # Get pod using label selector (much more reliable)
        pods = v1.list_namespaced_pod(namespace='default', label_selector=f"app={vnf_label}")
        
        if not pods.items:
            print(f"No pod found for label app={vnf_label}")
            is_crashed = crash_logger_global.get_count(vnf_key)
            return "0", "0", "0", "0", is_crashed

        pod = pods.items[0]  # Take the first (and usually only) pod
        pod_name = pod.metadata.name

        # Get resource requests from pod spec
        container = pod.spec.containers[0]
        cpu_request = container.resources.requests.get('cpu', '0') if container.resources.requests else '0'
        memory_request = container.resources.requests.get('memory', '0') if container.resources.requests else '0'

        # Try to get real usage with kubectl top (as fallback)
        try:
            result = subprocess.run(
                ["kubectl", "top", "pod", pod_name, "--containers", "--no-headers"],
                capture_output=True, text=True, timeout=5
            )
            
            if result.returncode == 0 and result.stdout.strip():
                parts = result.stdout.strip().split()
                cpu_usage = parts[1] if len(parts) > 1 else "0"
                memory_usage = parts[2] if len(parts) > 2 else "0"
            else:
                cpu_usage = "0"
                memory_usage = "0"
        except:
            cpu_usage = "0"
            memory_usage = "0"

        is_crashed_now = crash_logger_global.get_count(vnf_key)
        
        return cpu_request, memory_request, cpu_usage, memory_usage, is_crashed_now

    except Exception as e:
        print(f"Error retrieving {vnf_key} pod details: {e}")
        is_crashed_now = crash_logger_global.get_count(vnf_key)
        return "0", "0", "0", "0", is_crashed_now


from kubernetes import client, config
import time

# Load config ONCE at the top of your script (outside any function)
config.load_kube_config(config_file="/home/ubuntu/.kube/config")

v1 = client.CoreV1Api()
metrics_v1beta1 = client.CustomObjectsApi()   # For metrics

def parse_cpu(cpu_str: str) -> str:
    """Convert Kubernetes CPU (nanocores or cores) to millicores like '250m'"""
    if not cpu_str:
        return "0m"
    
    if cpu_str.endswith('n'):          # nanocores (most common from metrics)
        nanocores = int(cpu_str[:-1])
        millicores = nanocores // 1_000_000
        return f"{millicores}m"
    
    elif cpu_str.endswith('m'):        # already in millicores
        return cpu_str
    
    else:                              # plain cores (e.g. "1.5")
        try:
            cores = float(cpu_str)
            return f"{int(cores * 1000)}m"
        except:
            return "0m"


def parse_memory(mem_str: str) -> str:
    """Convert Kubernetes memory (Ki, Mi, Gi, etc.) to MiB like '80Mi'"""
    if not mem_str:
        return "0Mi"
    
    try:
        if mem_str.endswith('Ki'):
            kib = int(mem_str[:-2])
            mib = kib // 1024
            return f"{mib}Mi"
        
        elif mem_str.endswith('Mi'):
            return mem_str
        
        elif mem_str.endswith('Gi'):
            gib = int(mem_str[:-2])
            return f"{gib * 1024}Mi"
        
        else:
            # Assume bytes
            bytes_val = int(mem_str)
            mib = bytes_val // (1024 * 1024)
            return f"{mib}Mi"
    except:
        return "0Mi"
    

def get_pod_details(vnf_label: str, vnf_key: str):
    """Get pod details with nice formatted CPU (xxxm) and Memory (xxxMi)"""
    global crash_logger_global
    
    try:
        pods = v1.list_namespaced_pod(
            namespace="default",
            label_selector=f"app={vnf_label}"
        )
        
        if not pods.items:
            is_crashed = crash_logger_global.get_count(vnf_key)
            return "0", "0", "0m", "0Mi", is_crashed

        pod = pods.items[0]
        pod_name = pod.metadata.name

        # Resource Requests
        container = pod.spec.containers[0]
        cpu_request = container.resources.requests.get("cpu", "0") if container.resources and container.resources.requests else "0"
        mem_request = container.resources.requests.get("memory", "0") if container.resources and container.resources.requests else "0"

        # Real Usage from Metrics API
        cpu_usage = "0m"
        mem_usage = "0Mi"
        
        try:
            metrics = metrics_v1beta1.get_namespaced_custom_object(
                group="metrics.k8s.io",
                version="v1beta1",
                namespace="default",
                plural="pods",
                name=pod_name
            )
            
            container_metrics = metrics['containers'][0]['usage']
            cpu_usage = parse_cpu(container_metrics.get('cpu', '0'))
            mem_usage = parse_memory(container_metrics.get('memory', '0'))
            
        except Exception as e:
            print(f"Metrics API failed for {pod_name}: {e}")

        is_crashed_now = crash_logger_global.get_count(vnf_key)

        return cpu_request, mem_request, cpu_usage, mem_usage, is_crashed_now

    except Exception as e:
        print(f"Error retrieving {vnf_key} pod details: {e}")
        is_crashed = crash_logger_global.get_count(vnf_key)
        return "0", "0", "0m", "0Mi", is_crashed


# Then simplify your specific functions:
def get_pod_details_firewall():
    return get_pod_details("firewall-app", "firewall-app")

def get_pod_details_dpi():
    return get_pod_details("dpi-app", "dpi-app")

def get_pod_details_enc():
    return get_pod_details("enc-app", "enc-app")

def get_pod_details_comp():
    return get_pod_details("comp-app", "comp-app")

def get_pod_details_firewall2():
    return get_pod_details("firewall2-app", "firewall2-app")

def get_pod_details_nat():
    return get_pod_details("nat-app", "nat-app")


def run_get_crash_status_old(crash_logger_instance: CrashLogger, stop_event: threading.Event):
    global crash_logger_global
    crash_logger_global = crash_logger_instance
    print("Crash status monitor thread started.")
    while not stop_event.is_set():
        try:
            vnf_names = ["firewall-app", "dpi-app", "enc-app", "comp-app", "firewall2-app", "nat-app"]
            resource_usage = get_pod_resource_usage() # Relies on kubectl
            missing_vnfs = [vnf for vnf in vnf_names if vnf not in resource_usage]
            
            pods_status_dict = get_pods_status() # Relies on K8s client
            
            for worker_key, pod_info in pods_status_dict.items(): # worker_key is "worker1", etc.
                # pod_info is {"Name": "firewall-app-xxxx", "Status": "Running"}
                # Extract base VNF name like "firewall-app"
                pod_name_full = pod_info['Name'].split('-')[0] + '-app'
                status = pod_info.get('Status')
                
                # Match full pod name against vnf_names
                matched_vnf = None
                for vnf_base in vnf_names:
                    if pod_name_full.startswith(vnf_base):
                        matched_vnf = vnf_base
                        break
                
                if matched_vnf:
                    if matched_vnf in missing_vnfs or status in ["OOMKilled", "CrashLoopBackOff", "Error", "Failed"]:
                        crash_logger_instance.log_crash(matched_vnf, 1)
                    else:
                        crash_logger_instance.log_crash(matched_vnf, 0)
                else:
                    # This case should ideally not happen if worker_node_map is correct
                    # print(f"Warning: Pod {pod_name_full} from status update not matched to a VNF base name.")
                    pass
            
            # # For any VNF entirely missing from pod status (e.g. deleted), assume crashed
            # current_status_vnfs = {pod_info.get('Name', '').split('-')[0] + '-app' for pod_info in pods_status_dict.values() if pod_info.get('Name')}

            # for vnf_base in vnf_names:
            #     if vnf_base not in current_status_vnfs and vnf_base not in missing_vnfs: # if not in kubectl top AND not in pod list
            #         crash_logger_instance.log_crash(vnf_base, 1)


        except Exception as e:
            print(f"Error in crash_status thread: {e}")
        time.sleep(1)
    print("Crash status monitor thread stopped.")

def run_get_crash_status(crash_logger_instance: CrashLogger, stop_event: threading.Event):
    global crash_logger_global
    crash_logger_global = crash_logger_instance

    print("Crash status monitor thread started.")

    vnf_names = [
        "firewall-app",
        "dpi-app",
        "enc-app",
        "comp-app",
        "firewall2-app",
        "nat-app",
    ]

    prev_restart_counts = {vnf: None for vnf in vnf_names}
    seen_once = {vnf: False for vnf in vnf_names}

    while not stop_event.is_set():
        try:
            pod_status = get_pods_status()

            for vnf in vnf_names:
                crash_detected = 0

                if vnf not in pod_status:
                    # Only treat as crash if we have seen it before
                    if seen_once[vnf]:
                        crash_detected = 1
                    crash_logger_instance.log_crash(vnf, crash_detected)
                    continue

                info = pod_status[vnf]
                current_restarts = info["restart_count"]

                # First observation: use as baseline, do not count historical restarts
                if prev_restart_counts[vnf] is None:
                    prev_restart_counts[vnf] = current_restarts
                    seen_once[vnf] = True
                    crash_logger_instance.log_crash(vnf, 0)
                    continue

                restart_diff = current_restarts - prev_restart_counts[vnf]
                crash_detected = max(restart_diff, 0)

                # Optional fallback only for active bad states
                if crash_detected == 0 and info["status"] in ["CrashLoopBackOff", "Error", "Failed"]:
                    crash_detected = 1

                prev_restart_counts[vnf] = current_restarts
                seen_once[vnf] = True

                crash_logger_instance.log_crash(vnf, crash_detected)

        except Exception as e:
            print(f"Error in crash_status thread: {e}")

        time.sleep(1)

def get_pod_resource_usage():
    try:
        # Run the kubectl top pods command and capture its output
        result = subprocess.run(["sudo", "kubectl", "top", "pods"], capture_output=True, text=True)

        # Split the output into lines
        lines = result.stdout.split("\n")

        # Define the pods to track
        target_pods = ["firewall-app", "dpi-app", "enc-app", "comp-app", "firewall2-app", "nat-app"]

        # Initialize a dictionary to store CPU and memory usage
        pod_usage = {}

        # Iterate over each line and extract the usage
        for line in lines[1:]:  # Skip the header
            if not line.strip():
                continue  # Skip empty lines

            # Split the line into columns
            parts = line.split()
            if len(parts) < 3:
                continue  # Skip malformed lines

            # Get the pod name, CPU, and memory usage
            pod_name = parts[0]
            cpu_usage = parts[1]
            memory_usage = parts[2]

            # Check if the pod is in the target list
            for target in target_pods:
                if target in pod_name:
                    pod_usage[target] = {
                        "CPU": cpu_usage,
                        "Memory": memory_usage
                    }

        return pod_usage

    except Exception as e:
        print(f"Error: {e}")
        return None
    


def get_pods_status_old():

    list_pod_name = {}
    # Define a list of worker nodes and pod name keywords
    worker_node_map = {
        "worker1": "firewall-app",
        "worker2": "dpi-app",
        "worker3": "enc-app",
        "worker4": "comp-app",
        "worker5": "firewall2-app",
        "worker6": "nat-app"
    }
    i = 0

    while True:

        try:
            pod_list = None
            i = i+1
            print(i)

            # Load kube config
            config.load_kube_config(config_file="/home/ubuntu/.kube/config")
            # Create an instance of the Kubernetes client
            api_instance = client.CoreV1Api()
            # Retrieve the list of all pods in the cluster
            pod_list = api_instance.list_pod_for_all_namespaces()
        
            # Loop through the pods and check their status
            if pod_list and hasattr(pod_list, 'items') and pod_list.items:
                for pod in pod_list.items:
                    node_name = pod.spec.node_name
                    pod_name = pod.metadata.name
                    pod_phase = pod.status.phase
                    deletion_timestamp = pod.metadata.deletion_timestamp

                    # Check if the pod belongs to one of the worker nodes

                    if node_name in worker_node_map:
                        pod_keyword = worker_node_map[node_name]
                        pod_status = get_pod_status(pod, deletion_timestamp)

                        # Check if the pod name matches the keyword and pod status is "Running"
                        if pod_keyword in pod_name and (pod_status == "Running" or pod_status == "OOMKilled" or pod_status == "CrashLoopBackOff"):
                            list_pod_name[node_name] = {
                                "Name": pod_name,
                                "Status": pod_status
                            }
                break
                
        except Exception as e:
            print(f"Exception when calling CoreV1Api->list_pod_for_all_namespaces: {e}")
    
    return list_pod_name

def get_pods_status_old2():
    config.load_kube_config(config_file="/home/ubuntu/.kube/config")
    v1 = client.CoreV1Api()
    
    worker_node_map = {
        "worker1": "firewall-app",
        "worker2": "dpi-app",
        "worker3": "enc-app",
        "worker4": "comp-app",
        "worker5": "firewall2-app",
        "worker6": "nat-app"
    }
    
    list_pod_name = {}
    
    try:
        pods = v1.list_pod_for_all_namespaces(watch=False)
        
        for pod in pods.items:
            if pod.spec.node_name in worker_node_map:
                expected_keyword = worker_node_map[pod.spec.node_name]
                
                if expected_keyword in pod.metadata.name:
                    status = "Running"
                    if pod.metadata.deletion_timestamp:
                        status = "Terminating"
                    elif pod.status.phase != "Running":
                        status = pod.status.phase
                    
                    # Check for CrashLoopBackOff or OOMKilled
                    if pod.status.container_statuses:
                        for cs in pod.status.container_statuses:
                            if cs.state.waiting and cs.state.waiting.reason in ["CrashLoopBackOff", "ImagePullBackOff"]:
                                status = cs.state.waiting.reason
                            elif cs.state.terminated and cs.state.terminated.reason == "OOMKilled":
                                status = "OOMKilled"
                    
                    list_pod_name[pod.spec.node_name] = {
                        "Name": pod.metadata.name,
                        "Status": status
                    }
                    break  # We only need one matching pod per worker
                    
    except Exception as e:
        print(f"Error in get_pods_status: {e}")
    
    return list_pod_name

def get_pods_status():
    """
    Get pod status for all VNFs with restart count and OOM detection info.
    """
    config.load_kube_config(config_file="/home/ubuntu/.kube/config")
    v1 = client.CoreV1Api()

    vnf_labels = [
        "firewall-app",
        "dpi-app",
        "enc-app",
        "comp-app",
        "firewall2-app",
        "nat-app"
    ]

    pod_status = {}

    try:
        for label in vnf_labels:
            pods = v1.list_namespaced_pod(
                namespace="default",
                label_selector=f"app={label}"
            )

            if not pods.items:
                continue

            pod = pods.items[0]

            status = "Running"
            restart_count = 0
            oom_killed = False

            if pod.status.container_statuses:
                cs = pod.status.container_statuses[0]

                restart_count = cs.restart_count

                # Check current state
                if cs.state.waiting and cs.state.waiting.reason:
                    status = cs.state.waiting.reason

                elif cs.state.terminated and cs.state.terminated.reason:
                    status = cs.state.terminated.reason

                # 🔥 IMPORTANT: check last_state for OOM
                if cs.last_state and cs.last_state.terminated:
                    if cs.last_state.terminated.reason == "OOMKilled":
                        oom_killed = True

            pod_status[label] = {
                "pod_name": pod.metadata.name,
                "status": status,
                "restart_count": restart_count,
                "oom_killed": oom_killed
            }

    except Exception as e:
        print(f"Error in get_pods_status: {e}")

    return pod_status

def get_pod_status(pod, deletion_timestamp):
    """
    Function to get the pod status based on termination, OOMKilled, or CrashLoopBackOff.
    """
    pod_status = pod.status.phase

    # Check if the pod is terminating
    if deletion_timestamp:
        return "Terminating"

    # Check container statuses for OOMKilled or CrashLoopBackOff
    for container_status in pod.status.container_statuses:
        if container_status.state.terminated:
            terminated_state = container_status.state.terminated
            if terminated_state.reason == "OOMKilled":
                print(f"Pod {pod.metadata.name} on {pod.spec.node_name} was OOMKilled.")
                pod_status = "OOMKilled"

        if pod_status == "Running":
            if container_status.state.waiting and container_status.state.waiting.reason == "CrashLoopBackOff":
                print(f"Pod {pod.metadata.name} on {pod.spec.node_name} is in CrashLoopBackOff.")
                pod_status = "CrashLoopBackOff"

    return pod_status


def read_from_database(max_retries=5, retry_delay=1):
    """Read from database with timeout detection for crashed VNFs"""
    
    for attempt in range(max_retries):
        try:
            conn = sqlite3.connect('shared_data_temp_top2.db', timeout=10)
            cursor = conn.cursor()
            current_time = time.time()
        
            def safe_fetch_with_timeout(key, default='0'):
                """Fetch value and check if it's recent, otherwise return 0 (crashed)"""
                cursor.execute('SELECT value, timestamp FROM data WHERE key = ?', (key,))
                result = cursor.fetchone()
                
                if result is None:
                    print(f"Warning: No data for {key}, returning 0 (not initialized)")
                    return default
                
                value, timestamp = result
                age = current_time - timestamp
                
                # If data is older than timeout, VNF is considered crashed
                if age > TIMEOUT_SECONDS:
                    print(f"Warning: {key} data is {age:.1f}s old (timeout={TIMEOUT_SECONDS}s), VNF likely crashed, returning 0")
                    return '0'
                
                return value
            
            # Fetch all values with timeout detection
            processing_delay_firewall = safe_fetch_with_timeout('processing_delay_firewall')
            processing_delay_dpi = safe_fetch_with_timeout('processing_delay_dpi')
            processing_delay_encryption = safe_fetch_with_timeout('processing_delay_encryption')
            processing_delay_compression = safe_fetch_with_timeout('processing_delay_compression')
            processing_delay_firewall2 = safe_fetch_with_timeout('processing_delay_firewall2')
            processing_delay_nat = safe_fetch_with_timeout('processing_delay_nat')

            traffic_rate_firewall = safe_fetch_with_timeout('traffic_rate_firewall')
            traffic_rate_dpi = safe_fetch_with_timeout('traffic_rate_dpi')
            traffic_rate_encryption = safe_fetch_with_timeout('traffic_rate_encryption')
            traffic_rate_compression = safe_fetch_with_timeout('traffic_rate_compression')
            traffic_rate_firewall2 = safe_fetch_with_timeout('traffic_rate_firewall2')
            traffic_rate_nat = safe_fetch_with_timeout('traffic_rate_nat')

            average_rtt = safe_fetch_with_timeout('average_rtt')
            transmission_rate = safe_fetch_with_timeout('transmission_rate')
            
            cursor.close()
            conn.close()
            
            return (processing_delay_firewall, processing_delay_dpi, 
                    processing_delay_encryption, processing_delay_compression,
                    processing_delay_firewall2, processing_delay_nat, 
                    traffic_rate_firewall, traffic_rate_dpi, 
                    traffic_rate_encryption, traffic_rate_compression,
                    traffic_rate_firewall2, traffic_rate_nat, 
                    average_rtt, transmission_rate)
            
        except sqlite3.OperationalError as e:
            if "database is locked" in str(e) and attempt < max_retries - 1:
                print(f"Database locked, retry {attempt + 1}/{max_retries}")
                time.sleep(retry_delay)
                continue
            print(f"Database error after {max_retries} retries: {e}")
            raise
        except Exception as e:
            print(f"Unexpected error reading database: {e}")
            cursor.close()
            conn.close()
            raise
    
    # If all retries fail, return default values
    print("Warning: All database read retries failed, returning all zeros")
    return ('0', '0', '0', '0', '0', '0', '0', '0', '0', '0')

def initialize_database():
    """Initialize the VNF KPI database"""
    conn = sqlite3.connect('VNF_KPI_database_crash_top2_v2_statefull_dyn_15May.db', timeout=10)
    cursor = conn.cursor()
    
    create_table_query = """
    CREATE TABLE IF NOT EXISTS VNF_KPI_database (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        Timestamp TEXT,
        VNF_name_firewall TEXT, cpu_request_firewall REAL, cpu_usage_firewall REAL, 
        memory_request_firewall REAL, memory_usage_firewall REAL, 
        processing_delay_firewall REAL, traffic_rate_firewall REAL, crash_firewall REAL,
        VNF_name_dpi TEXT, cpu_request_dpi REAL, cpu_usage_dpi REAL, 
        memory_request_dpi REAL, memory_usage_dpi REAL, 
        processing_delay_dpi REAL, traffic_rate_dpi REAL, crash_dpi REAL,
        VNF_name_enc TEXT, cpu_request_enc REAL, cpu_usage_enc REAL, 
        memory_request_enc REAL, memory_usage_enc REAL, 
        processing_delay_enc REAL, traffic_rate_enc REAL, crash_enc REAL,
        VNF_name_comp TEXT, cpu_request_comp REAL, cpu_usage_comp REAL, 
        memory_request_comp REAL, memory_usage_comp REAL, 
        processing_delay_comp REAL, traffic_rate_comp REAL, crash_comp REAL,
        VNF_name_firewall2 TEXT, cpu_request_firewall2 REAL, cpu_usage_firewall2 REAL, 
        memory_request_firewall2 REAL, memory_usage_firewall2 REAL, 
        processing_delay_firewall2 REAL, traffic_rate_firewall2 REAL, crash_firewall2 REAL,
        VNF_name_nat TEXT, cpu_request_nat REAL, cpu_usage_nat REAL, 
        memory_request_nat REAL, memory_usage_nat REAL, 
        processing_delay_nat REAL, traffic_rate_nat REAL, crash_nat REAL,
        average_rtt REAL, transmission_rate REAL
    )
    """
    cursor.execute(create_table_query)
    conn.commit()
    conn.close()

def initialize_kpi_database():
    """Initialize the shared KPI database with default values"""
    conn = sqlite3.connect('shared_data_temp_top2.db', check_same_thread=False, timeout=10)
    cursor = conn.cursor()
    cursor.execute('CREATE TABLE IF NOT EXISTS data (key TEXT PRIMARY KEY, value TEXT, timestamp REAL)')
    
    # Initialize all keys with default values and old timestamp
    default_keys = [
        'processing_delay_firewall', 'traffic_rate_firewall',
        'processing_delay_dpi', 'traffic_rate_dpi',
        'processing_delay_encryption', 'traffic_rate_encryption',
        'processing_delay_compression', 'traffic_rate_compression',
        'processing_delay_firewall2', 'traffic_rate_firewall2',
        'processing_delay_nat', 'traffic_rate_nat',
        'average_rtt', 'transmission_rate'
    ]
    
    # Use old timestamp so they'll be considered stale until first update
    old_timestamp = time.time() - (TIMEOUT_SECONDS + 1)
    
    for key in default_keys:
        cursor.execute('INSERT OR IGNORE INTO data (key, value, timestamp) VALUES (?, ?, ?)', 
                      (key, '0', old_timestamp))
    
    conn.commit()
    conn.close()
    print("KPI database initialized with default values")

def logging_pod_resources():
    """Main logging loop that collects all metrics"""
    initialize_database()
    config.load_kube_config(config_file="/home/ubuntu/.kube/config")
    v1 = client.CoreV1Api()

    def run_in_parallel():
        with concurrent.futures.ThreadPoolExecutor() as executor:
            future_firewall = executor.submit(get_pod_details_firewall)
            future_dpi = executor.submit(get_pod_details_dpi)
            future_enc = executor.submit(get_pod_details_enc)
            future_comp = executor.submit(get_pod_details_comp)
            future_firewall2 = executor.submit(get_pod_details_firewall2)
            future_nat = executor.submit(get_pod_details_nat)
            future_kpis = executor.submit(read_from_database)

            # Get results
            cpu_request_firewall, memory_request_firewall, cpu_usage_firewall, memory_usage_firewall, crash_firewall = future_firewall.result()
            cpu_request_dpi, memory_request_dpi, cpu_usage_dpi, memory_usage_dpi, crash_dpi = future_dpi.result()
            cpu_request_enc, memory_request_enc, cpu_usage_enc, memory_usage_enc, crash_enc = future_enc.result()
            cpu_request_comp, memory_request_comp, cpu_usage_comp, memory_usage_comp, crash_comp = future_comp.result()
            cpu_request_firewall2, memory_request_firewall2, cpu_usage_firewall2, memory_usage_firewall2, crash_firewall2 = future_firewall2.result()
            cpu_request_nat, memory_request_nat, cpu_usage_nat, memory_usage_nat, crash_nat = future_nat.result()
            (processing_delay_firewall, processing_delay_dpi, processing_delay_encryption, 
             processing_delay_compression, processing_delay_firewall2, processing_delay_nat, traffic_rate_firewall, traffic_rate_dpi, 
             traffic_rate_encryption, traffic_rate_compression, traffic_rate_firewall2, traffic_rate_nat,
             average_rtt, transmission_rate) = future_kpis.result()
        
        return (cpu_request_firewall, memory_request_firewall, cpu_usage_firewall, memory_usage_firewall, crash_firewall,
                cpu_request_dpi, memory_request_dpi, cpu_usage_dpi, memory_usage_dpi, crash_dpi,
                cpu_request_enc, memory_request_enc, cpu_usage_enc, memory_usage_enc, crash_enc,
                cpu_request_comp, memory_request_comp, cpu_usage_comp, memory_usage_comp, crash_comp,
                cpu_request_firewall2, memory_request_firewall2, cpu_usage_firewall2, memory_usage_firewall2, crash_firewall2,
                cpu_request_nat, memory_request_nat, cpu_usage_nat, memory_usage_nat, crash_nat,
                processing_delay_firewall, processing_delay_dpi, processing_delay_encryption, 
                processing_delay_compression, processing_delay_firewall2, processing_delay_nat, traffic_rate_firewall, traffic_rate_dpi, 
                traffic_rate_encryption, traffic_rate_compression, traffic_rate_firewall2, traffic_rate_nat,
                average_rtt, transmission_rate)

    while True:
        try:
            results = run_in_parallel()

            (cpu_request_firewall, memory_request_firewall, cpu_usage_firewall, memory_usage_firewall, crash_firewall,
             cpu_request_dpi, memory_request_dpi, cpu_usage_dpi, memory_usage_dpi, crash_dpi,
             cpu_request_enc, memory_request_enc, cpu_usage_enc, memory_usage_enc, crash_enc,
             cpu_request_comp, memory_request_comp, cpu_usage_comp, memory_usage_comp, crash_comp,
             cpu_request_firewall2, memory_request_firewall2, cpu_usage_firewall2, memory_usage_firewall2, crash_firewall2,
             cpu_request_nat, memory_request_nat, cpu_usage_nat, memory_usage_nat, crash_nat,
             processing_delay_firewall, processing_delay_dpi, processing_delay_encryption, 
             processing_delay_compression, processing_delay_firewall2, processing_delay_nat, traffic_rate_firewall, traffic_rate_dpi, 
             traffic_rate_encryption, traffic_rate_compression, traffic_rate_firewall2, traffic_rate_nat,
             average_rtt, transmission_rate) = results

            # Insert into database
            conn = sqlite3.connect('VNF_KPI_database_crash_top2_v2_statefull_dyn_15May.db', timeout=10)
            cursor = conn.cursor()
            
            cursor.execute("""
            INSERT INTO VNF_KPI_database (
                Timestamp, 
                VNF_name_firewall, cpu_request_firewall, cpu_usage_firewall, 
                memory_request_firewall, memory_usage_firewall, 
                processing_delay_firewall, traffic_rate_firewall, crash_firewall,
                VNF_name_dpi, cpu_request_dpi, cpu_usage_dpi, 
                memory_request_dpi, memory_usage_dpi, 
                processing_delay_dpi, traffic_rate_dpi, crash_dpi,
                VNF_name_enc, cpu_request_enc, cpu_usage_enc, 
                memory_request_enc, memory_usage_enc, 
                processing_delay_enc, traffic_rate_enc, crash_enc,
                VNF_name_comp, cpu_request_comp, cpu_usage_comp, 
                memory_request_comp, memory_usage_comp, 
                processing_delay_comp, traffic_rate_comp, crash_comp,
                VNF_name_firewall2, cpu_request_firewall2, cpu_usage_firewall2, 
                memory_request_firewall2, memory_usage_firewall2, 
                processing_delay_firewall2, traffic_rate_firewall2, crash_firewall2,
                VNF_name_nat, cpu_request_nat, cpu_usage_nat, 
                memory_request_nat, memory_usage_nat, 
                processing_delay_nat, traffic_rate_nat, crash_nat,
                average_rtt, transmission_rate
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """, (
                time.strftime('%Y-%m-%d %H:%M:%S'),
                "firewall_app", cpu_request_firewall, cpu_usage_firewall, 
                memory_request_firewall, memory_usage_firewall, 
                processing_delay_firewall, traffic_rate_firewall, crash_firewall,
                "dpi_app", cpu_request_dpi, cpu_usage_dpi, 
                memory_request_dpi, memory_usage_dpi, 
                processing_delay_dpi, traffic_rate_dpi,  crash_dpi,
                "enc_app", cpu_request_enc, cpu_usage_enc, 
                memory_request_enc, memory_usage_enc, 
                processing_delay_encryption, traffic_rate_encryption, crash_enc,
                "comp_app", cpu_request_comp, cpu_usage_comp, 
                memory_request_comp, memory_usage_comp, 
                processing_delay_compression, traffic_rate_compression, crash_comp,
                "firewall2_app", cpu_request_firewall2, cpu_usage_firewall2, 
                memory_request_firewall2, memory_usage_firewall2, 
                processing_delay_firewall2, traffic_rate_firewall2, crash_firewall2,
                "nat_app", cpu_request_nat, cpu_usage_nat, 
                memory_request_nat, memory_usage_nat, 
                processing_delay_nat, traffic_rate_nat, crash_nat,
                average_rtt, transmission_rate
            ))
            conn.commit()
            conn.close()

        except Exception as e:
            print(f"Error in logging loop: {e}")

        time.sleep(interval)

def run_flask_app(port):
    app.run(host='0.0.0.0', port=port)

if __name__ == '__main__':
    # Initialize databases first
    initialize_kpi_database()

    global crash_logger_global

    crash_logger_global = CrashLogger()
    stop_event = threading.Event()
    
    crash_thread = threading.Thread(
        target=run_get_crash_status,
        args=(crash_logger_global, stop_event),
        daemon=True
    )

    crash_thread.start()
    
    # Start logging thread
    packet_handler_thread = threading.Thread(target=logging_pod_resources)
    packet_handler_thread.daemon = True
    packet_handler_thread.start()

    # Start Flask on multiple ports
    ports = [5001, 5002, 5003, 5004, 5005, 5006, 5007, 5008]
    threads = []

    for port in ports:
        thread = threading.Thread(target=run_flask_app, args=(port,))
        threads.append(thread)
        thread.start()

    for thread in threads:
        thread.join()