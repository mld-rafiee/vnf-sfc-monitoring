## A Kubernetes‑native testbed for monitoring performance of Virtual Network Functions (VNFs) in Service Function Chains (SFCs).

**Designed for edge/5G environments, it generates IoT traffic, measures application‑level (latency), Network-level (traffic load) and infrastructure‑level (CPU, memory) metrics, and detects failures.**

[![License](https://img.shields.io/badge/license-MIT-blue.svg)](LICENSE)
[![Python](https://img.shields.io/badge/python-3.8%2B-blue)](https://www.python.org/)
[![Kubernetes](https://img.shields.io/badge/kubernetes-1.24%2B-blue)](https://kubernetes.io/)




## Overview

Monitoring VNFs in a dynamic SFC is critical for ensuring QoS in 5G and edge networks. This project provides:

- Two distinct SFCs (Firewall → DPI → Encryption → Compression and Firewall → DPI → NAT) deployed on Kubernetes.
- A scalable IoT traffic generator (1000–50,000 IoT devices, 4–50 msg/s).
- A monitoring service that collects both application metrics (processing delay, traffic load) and infrastructure metrics (CPU/memory usage) from each VNF.
- Failure detection (crash events) and persistent storage in SQLite.
- All components are containerised and orchestrated with Kubernetes + Kustomize.

## Components

- **VNFs**: Five network functions (Firewall, DPI, Encryption, Compression, NAT) implemented as Python services.
- **Kubernetes Manifests**: Kustomize‑based deployment files for multi‑node computing continuum environment.
- **Traffic Generator**: Emulates IoT sensor data at scale using real‑world bandwidth traces.
- **Performance Metrics Monitor**: monitoring server that monitors VNF metrics, Kubernetes pod stats, and crash events.


## VNFs

| VNF          | Description                                           | SFC(s)       |
|--------------|-------------------------------------------------------|--------------|
| **Firewall** | Filters packets based on rules.                       | SFC 1        |
| **DPI**      | Deep Packet Inspection – classifies traffic.          | Both         |
| **Encryption**| Encrypts payloads.                                   | SFC 1        |
| **Compression**| Compresses data to reduce size.                     | SFC 1        |
| **NAT**      | Network Address Translation.                          | SFC 2        |

Each VNF is containerised and runs as a Kubernetes Deployment with a Service for internal routing.

## 🚀 Deployment

This project uses **Kustomize** (built into `kubectl`) to manage the Kubernetes manifests for all six VNF instances across the two Service Function Chains (SFCs).

### Prerequisites

- A Kubernetes cluster.
- `kubectl` configured with access to the cluster.
- **6 worker nodes** labeled with the following hostnames (update `nodeSelector` values in the overlays if your nodes have different names):
  - `worker1` → Firewall (SFC 1)
  - `worker2` → DPI (Shared)
  - `worker3` → Encryption (SFC 1)
  - `worker4` → Compression (SFC 1)
  - `worker5` → Firewall 2 (SFC 2)
  - `worker6` → NAT (SFC 2)

### Deploy the Full SFC

Navigate to the repository root and run the following command to deploy **all six VNFs** simultaneously:

```bash
kubectl apply -k ./k8s/overlays/
```

## 🚦 Traffic Generation

The IoT data generation layer simulates sensor traffic at configurable scales (1,000–50,000 devices) and rates (4–50 msgs/s), using real 4G/5G bandwidth traces from the NortNetEdge dataset.

### Run the Traffic Generator

Navigate to the generator directory and run the orchestrator:

```bash
cd traffic-generator
python orchestrator_SFC1.py
python orchestrator_SFC2.py
```

## 📊 VNF Performance Monitor

The performance monitor is a centralized observability service that monitor, aggregates, and stores real‑time operational data from the entire Service Function Chain.

Located in `vnf-performance-monitor/`, it runs as a multi‑threaded Flask server that continuously tracks both **application‑level performance metrics** (reported directly by the VNFs) and **infrastructure‑level metrics** (scraped from the Kubernetes API).

### Key Capabilities

- **Application metrics**: Each VNF pushes its processing delay and traffic load to dedicated REST endpoints.
- **Resource Telemetry**: Directly queries the Kubernetes Metrics API to capture actual CPU and memory usage for each pod, alongside the requested resource limits.
- **Crash Detection**: Monitors pod restarts, `OOMKilled` events, and states like `CrashLoopBackOff` to instantly flag reliability issues.
- **Time‑Series Storage**: All incoming metrics are written to a local SQLite database, timestamped, and structured for easy querying and post‑experiment analysis.


### Run the Performance Monitor

Navigate to the monitor directory and start the service:

```bash
cd vnf-performance-monitor
python Monitor_performance_VNF.py
```