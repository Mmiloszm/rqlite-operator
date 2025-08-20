# rqlite-operator

A Kubernetes operator for managing [rqlite](https://rqlite.io/) (replicated SQLite) clusters, built using the Kubebuilder framework. This operator provides comprehensive management of rqlite clusters with advanced features including auto-scaling, backup/restore operations, rolling updates, and deep observability.

## Description

The rqlite-operator implements the full spectrum of [Operator Capability Levels](https://operatorframework.io/operator-capabilities/), from basic installation and configuration (Level 1) to basic auto pilot scaling (Level 5). It provides Custom Resource Definitions (CRDs) and controllers for managing rqlite clusters, backups, scheduled backups, and restores in Kubernetes environments.

## Features Overview

### Level 1: Basic Install
- **StatefulSet Management**: Automated deployment and lifecycle management of rqlite clusters
- **Service Discovery**: Headless and client services for cluster networking
- **Persistent Storage**: PVC management with configurable storage classes and capacities
- **Pod Anti-Affinity**: Intelligent pod placement across nodes for high availability

### Level 2: Seamless Upgrades  
- **Rolling Updates**: Leader-aware rolling updates with minimal downtime
- **Version Validation**: Automatic validation of version compatibility
- **Update Strategies**: Configurable update policies (LeaderLast, LeaderFirst, Automatic)
- **Rollback Support**: Automatic rollback on failed updates
- **Pod Disruption Budgets**: Maintains cluster availability during updates

### Level 3: Full Lifecycle
- **Backup Operations**: One-time and scheduled backup management via Jobs
- **Restore Operations**: Point-in-time cluster restoration from backups
- **Disaster Recovery**: Complete cluster recovery with data integrity
- **Storage Management**: Automatic storage expansion based on usage thresholds

### Level 4: Deep Insights & Monitoring
- **Prometheus Integration**: custom metrics for comprehensive monitoring
- **Auto-scaling**: HPA with latency-based and resource-based scaling
- **Quorum-Aware Scaling**: Maintains odd replica counts for Raft consensus
- **Grafana Dashboards**: Pre-built visualization dashboards

### Level 5: Auto Pilot
- **Auto-scaling**: HPA with latency-based and resource-based scaling
- **Quorum-Aware Scaling**: Maintains odd replica counts for Raft consensus

## Quick Start

### Prerequisites
- Kubernetes cluster v1.11.3+
- kubectl configured to access your cluster
- Go 1.23.0+ (for development)
- Docker 17.03+ (for building images)

### Installation

For local development and testing, build and deploy the operator locally:

1. **Build the operator image:**
   ```bash
   make docker-build IMG=rqlite-operator:latest
   ```

2. **Load the image into your cluster** (varies by cluster type):
   ```bash
   # For kind clusters
   kind load docker-image rqlite-operator:latest
   
   # For k3d clusters
   k3d image import rqlite-operator:latest
   
   # For minikube clusters
   minikube image load rqlite-operator:latest
   
   # For k3s clusters (requires additional setup)
   # Save image as tar and import
   docker save rqlite-operator:latest | sudo k3s ctr images import -
   
   # For Docker Desktop - images are automatically available
   ```

3. **Deploy the operator:**
   ```bash
   make deploy IMG=rqlite-operator:latest
   ```

#### Next Steps

1. **Create your first rqlite cluster:**
   ```bash
   kubectl apply -f config/samples/basic/rqlitecluster_basic.yaml
   ```

4. **Verify the cluster is running:**
   ```bash
   kubectl get rqlitecluster
   kubectl get pods -l app.kubernetes.io/name=rqlite
   ```

### Access Your Cluster

Once deployed, you can access your rqlite cluster:

```bash
# Port-forward to access the cluster
kubectl port-forward svc/basic-cluster-client 4001:4001

# Test the cluster
curl -X POST http://localhost:4001/db/execute \
  -H "Content-Type: application/json" \
  -d '["CREATE TABLE test (id INTEGER, name TEXT)"]'

curl -X POST http://localhost:4001/db/query \
  -H "Content-Type: application/json" \
  -d '["SELECT * FROM test"]'
```

## Feature Documentation

### Basic Cluster Configuration

Deploy a simple 3-node rqlite cluster:

```yaml
apiVersion: rqlite.example.com/v1alpha1
kind: RqliteCluster
metadata:
  name: basic-cluster
spec:
  size: 3
  version: "latest"
  storageClassName: "standard"
  storageCapacity: "1Gi"
```

**Sample Location**: `config/samples/basic/rqlitecluster_basic.yaml`

### Auto-Scaling Configuration

Enable intelligent auto-scaling with latency and resource metrics:

```yaml
apiVersion: rqlite.example.com/v1alpha1
kind: RqliteCluster
metadata:
  name: autoscaling-cluster
spec:
  size: 3
  version: "latest"
  autoScaling:
    enabled: true
    hpa:
      enabled: true
      minReplicas: 3
      maxReplicas: 9
      targetCPUUtilizationPercentage: 70
      targetMemoryUtilizationPercentage: 80
      customMetrics:
        - name: rqlite_p95_read_latency_ms
          type: External
          targetValue: "10"
          description: "P95 read query latency in ms"
    storageExpansion:
      enabled: true
      thresholdPercentage: 80
      expansionPercentage: 50
```

**Sample Location**: `config/samples/autoscaling/rqlitecluster_autoscaling.yaml`

### Backup and Restore

Create backups and restore operations:

```yaml
# One-time backup
apiVersion: rqlite.example.com/v1alpha1
kind: RqliteBackup
metadata:
  name: manual-backup
spec:
  clusterName: basic-cluster
  storage:
    pvc:
      name: backup-pvc # your pvc
      filename: manual-backup.db
  
---
# Continuous backup (for point-in-time recovery)
apiVersion: rqlite.example.com/v1alpha1
kind: RqliteContinuousBackup
metadata:
  name: continuous-backup
spec:
  clusterName: basic-cluster
  interval: "1m"                        # Create backup every minute
  retentionPeriod: "5m"                 # Keep last 5 minutes of backup history
  storagePVC: backup-archive-storage    # PVC for backup storage
  suspend: false                        # Keep backup active

---
# Restore from specific backup
apiVersion: rqlite.example.com/v1alpha1
kind: RqliteRestore
metadata:
  name: restore-from-backup
spec:
  clusterName: basic-cluster
  backupName: manual-backup             # Name of backup to restore from

---
# Point-in-time restore using continuous backup
apiVersion: rqlite.example.com/v1alpha1
kind: RqliteRestore
metadata:
  name: restore-point-in-time
spec:
  clusterName: basic-cluster
  pointInTime: "2025-08-14T10:00:00Z"         # Restore to specific timestamp
  continuousBackupName: continuous-backup     # Continuous backup to use for PITR
```

**Sample Locations**: 
- `config/samples/backup/rqlitebackup_manual.yaml`
- `config/samples/backup/rqlitecontinuousbackup_test.yaml`
- `config/samples/backup/rqliterestore_example.yaml`

### Rolling Updates

Configure advanced rolling update behavior:

```yaml
apiVersion: rqlite.example.com/v1alpha1
kind: RqliteCluster
metadata:
  name: rolling-update-cluster
spec:
  size: 5
  version: "7.21.4"
  updateStrategy:
    type: "RollingUpdate"
    rollingUpdate:
      maxUnavailable: 1
      leaderUpdatePolicy: "LeaderLast"
      updateTimeout: 300
      rollbackOnFailure: true
```

**Sample Location**: `config/samples/advanced/rqlitecluster_rolling_updates.yaml`

### High Availability Configuration

Deploy with advanced HA settings:

> **Note**: High availability features require a multi-node Kubernetes cluster. For local development, use tools like [k3d](https://k3d.io/), [kind](https://kind.sigs.k8s.io/), or [minikube](https://minikube.sigs.k8s.io/) with multiple nodes to test HA configurations.

```yaml
apiVersion: rqlite.example.com/v1alpha1
kind: RqliteCluster
metadata:
  name: ha-cluster
spec:
  size: 5
  version: "latest"
  podAntiAffinity:
    enabled: true
    type: "hard"
    topologyKey: "kubernetes.io/hostname"
  updateStrategy:
    type: "RollingUpdate"
    rollingUpdate:
      maxUnavailable: 1
      leaderUpdatePolicy: "LeaderLast"
```

**Sample Location**: `config/samples/advanced/rqlitecluster_ha.yaml`

### Monitoring and Observability

Enable comprehensive monitoring:

```yaml
apiVersion: rqlite.example.com/v1alpha1
kind: RqliteCluster
metadata:
  name: monitored-cluster
  annotations:
    prometheus.io/scrape: "true"
    prometheus.io/port: "8080"
spec:
  size: 3
  version: "latest"
  autoScaling:
    enabled: true
    hpa:
      enabled: true
      minReplicas: 3
      maxReplicas: 9
      customMetrics:
        - name: rqlite_p95_read_latency_ms
          type: External
          targetValue: "10"
```

**Sample Location**: `config/samples/monitoring/rqlitecluster_monitoring.yaml`

## Sample Files Organization

The `config/samples` directory is organized by feature:

```
config/samples/
├── basic/
│   └── rqlitecluster_basic.yaml           # Simple 3-node cluster
├── autoscaling/
│   ├── rqlitecluster_autoscaling.yaml     # HPA with custom metrics
│   └── rqlitecluster_storage_expansion.yaml # Storage auto-expansion
├── backup/
│   ├── rqlitebackup_manual.yaml           # One-time backup
│   ├── rqlitescheduledbackup_daily.yaml   # Daily scheduled backup
│   └── rqliterestore_example.yaml         # Restore operation
├── advanced/
│   ├── rqlitecluster_ha.yaml              # High availability setup
│   ├── rqlitecluster_rolling_updates.yaml # Advanced update strategies
│   └── rqlitecluster_custom_config.yaml   # Custom configurations
├── monitoring/
│   ├── rqlitecluster_monitoring.yaml      # Monitoring-enabled cluster
│   └── prometheus_config.yaml             # Prometheus configuration
└── production/
    ├── rqlitecluster_production.yaml      # Production-ready setup
    └── rqlitecluster_large_scale.yaml     # Large scale deployment
```

## Testing Samples

To test different features:

```bash
# Basic cluster
kubectl apply -f config/samples/basic/

# Auto-scaling features
kubectl apply -f config/samples/autoscaling/

# Backup operations
kubectl apply -f config/samples/backup/

# Production setup
kubectl apply -f config/samples/production/

# Clean up
kubectl delete -f config/samples/basic/
```

## Development

### Running locally

```bash
# Install CRDs
make install

# Run the operator locally. IMPORTANT! The Prometheus Oerator does not work when cluster is deployed in this way.
# Check Installation section to learn how to deploy the operator to work well with Prometheus Operator.
make run

# In another terminal, create a cluster
kubectl apply -f config/samples/basic/rqlitecluster_basic.yaml
```

## Monitoring Setup

To enable full monitoring capabilities with auto-scaling support:

1. **Install Prometheus Operator:**
   ```bash
   helm repo add prometheus-community https://prometheus-community.github.io/helm-charts
   helm install prometheus prometheus-community/kube-prometheus-stack
   ```

2. **Install Prometheus Adapter for Custom Metrics (required for auto-scaling):**
   ```bash
   helm install prometheus-adapter prometheus-community/prometheus-adapter \
     --set prometheus.url=http://prometheus-kube-prometheus-prometheus.default.svc.cluster.local
   ```

3. **Apply custom metrics configuration:**
   ```bash
   kubectl apply -f config/samples/monitoring/prometheus-adapter-config-patch.yaml
   kubectl rollout restart deployment prometheus-adapter
   ```

4. **Apply monitoring configuration:**
   ```bash
   kubectl apply -f config/monitoring/servicemonitor.yaml
   kubectl apply -f config/monitoring/prometheus-rules.yaml
   
   # Add required label for Prometheus to discover ServiceMonitor
   kubectl label servicemonitor -n rqlite-operator-system rqlite-operator-metrics release=prometheus
   ```

5. **Verify custom metrics are working:**
   ```bash
   # Check if custom metrics API is available
   kubectl get --raw "/apis/custom.metrics.k8s.io/v1beta1"
   
   # Test read latency metric (replace 'your-cluster' with actual cluster name)
   kubectl get --raw "/apis/custom.metrics.k8s.io/v1beta1/namespaces/default/rqliteclusters.rqlite.example.com/your-cluster/rqlite_p95_read_latency_ms"
   ```

6. **Import Grafana dashboard:**
   ```bash
   # Port-forward to access Grafana
   kubectl port-forward svc/prometheus-grafana 3000:80 &
   
   # Access Grafana at http://localhost:3000
   # Login credentials: admin / prom-operator
   
   # Import dashboard via API
   curl -X POST \
     http://admin:prom-operator@localhost:3000/api/dashboards/db \
     -H 'Content-Type: application/json' \
     -d @config/monitoring/grafana-dashboard.json
   
   # Or import manually via GUI at http://localhost:3000/dashboard/import
   # using config/monitoring/grafana-dashboard-gui-import.json
   ```

## License

Copyright 2025.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.