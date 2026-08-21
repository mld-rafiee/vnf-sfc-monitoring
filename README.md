# VNF-SFC-Monitoring

## 🧩 Components

- **VNFs**: Five network functions (Firewall, DPI, Encryption, Compression, NAT) implemented as Python services.
- **Kubernetes Manifests**: Kustomize‑based deployment files for multi‑node computing continuum environment.
- **Traffic Generator**: Emulates IoT sensor data at scale using real‑world bandwidth traces.
- **Performance Metrics Monitor**: monitoring server that monitors VNF metrics, Kubernetes pod stats, and crash events.


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