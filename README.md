# VNF-SFC-Monitoring

## 🚀 Deployment

This project uses **Kustomize** (built into `kubectl`) to manage the Kubernetes manifests for all six VNF instances across the two Service Function Chains (SFCs).

### Prerequisites

- A Kubernetes cluster (v1.14+).
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
