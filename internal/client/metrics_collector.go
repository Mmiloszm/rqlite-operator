package client

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"time"

	rqlitev1alpha1 "github.com/mmiloszm/rqlite-operator/api/v1alpha1"
	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/log"
	"sigs.k8s.io/controller-runtime/pkg/metrics"

	"github.com/prometheus/client_golang/prometheus"
)

const (
	RaftStateFollower  = "Follower"
	RaftStateCandidate = "Candidate"
	RaftStateLeader    = "Leader"
)

type RqliteMetrics struct {
	// Cluster-level metrics
	ClusterHealth *prometheus.GaugeVec
	ClusterSize   *prometheus.GaugeVec
	LeaderChanges *prometheus.CounterVec
	QuorumStatus  *prometheus.GaugeVec

	// Node-level metrics
	NodeStatus       *prometheus.GaugeVec
	NodeReachability *prometheus.GaugeVec
	RaftState        *prometheus.GaugeVec

	// Performance metrics
	QueryLatency    *prometheus.HistogramVec
	ReadLatency     *prometheus.HistogramVec
	DatabaseSize    *prometheus.GaugeVec
	ConnectionCount *prometheus.GaugeVec

	// Raft consensus metrics
	RaftIndex        *prometheus.GaugeVec
	RaftTerm         *prometheus.GaugeVec
	RaftCommitIndex  *prometheus.GaugeVec
	RaftAppliedIndex *prometheus.GaugeVec

	// Storage metrics
	DiskUsage     *prometheus.GaugeVec
	BackupCount   *prometheus.GaugeVec
	LastBackupAge *prometheus.GaugeVec

	// Auto-scaling metrics
	HPAStatus         *prometheus.GaugeVec
	ScalingEvents     *prometheus.CounterVec
	StorageExpansions *prometheus.CounterVec

	// Database Operations Metrics (from /debug/vars)
	DBExecutions      *prometheus.CounterVec
	DBQueries         *prometheus.CounterVec
	DBExecutionErrors *prometheus.CounterVec
	DBQueryErrors     *prometheus.CounterVec

	// Backup Discovery Metrics
	BackupInfo *prometheus.GaugeVec

	// Leadership Health Metrics (from /status)
	RaftLastContact   *prometheus.GaugeVec
	RaftLeaderChanges *prometheus.CounterVec

	// Database Size Metrics (from /status)
	SQLiteDBSize  *prometheus.GaugeVec
	SQLiteWALSize *prometheus.GaugeVec

	// Connection Pool Metrics (from /status)
	SQLiteConnectionsOpen *prometheus.GaugeVec
	SQLiteConnectionsIdle *prometheus.GaugeVec
	SQLiteConnectionsWait *prometheus.CounterVec
}

type RqliteNodesResponse map[string]struct {
	Addr         string  `json:"addr"`
	Reachable    bool    `json:"reachable"`
	Leader       bool    `json:"leader"`
	Time         float64 `json:"time"`
	ErrorMessage string  `json:"error_message,omitempty"`
}

type RqliteDebugVarsResponse struct {
	DB    DatabaseStats `json:"db"`
	Store StoreStats    `json:"store"`
}

type DatabaseStats struct {
	Executions      int64 `json:"executions"`
	Queries         int64 `json:"queries"`
	ExecutionErrors int64 `json:"execution_errors"`
	QueryErrors     int64 `json:"query_errors"`
}

type StoreStats struct {
	LeaderChanges int64 `json:"leader_changes_observed"`
}

type MetricsCollector struct {
	Client  client.Client
	Metrics *RqliteMetrics
}

func NewMetricsCollector(client client.Client) *MetricsCollector {
	m := &RqliteMetrics{
		ClusterHealth: prometheus.NewGaugeVec(
			prometheus.GaugeOpts{
				Name: "rqlite_cluster_health",
				Help: "Overall cluster health score (0-1)",
			},
			[]string{"cluster", "namespace"},
		),
		ClusterSize: prometheus.NewGaugeVec(
			prometheus.GaugeOpts{
				Name: "rqlite_cluster_size",
				Help: "Number of nodes in the cluster",
			},
			[]string{"cluster", "namespace"},
		),
		LeaderChanges: prometheus.NewCounterVec(
			prometheus.CounterOpts{
				Name: "rqlite_leader_changes_total",
				Help: "Total number of leader changes",
			},
			[]string{"cluster", "namespace"},
		),
		QuorumStatus: prometheus.NewGaugeVec(
			prometheus.GaugeOpts{
				Name: "rqlite_quorum_status",
				Help: "Quorum status (1=healthy, 0=unhealthy)",
			},
			[]string{"cluster", "namespace"},
		),

		NodeStatus: prometheus.NewGaugeVec(
			prometheus.GaugeOpts{
				Name: "rqlite_node_status",
				Help: "Node status (1=up, 0=down)",
			},
			[]string{"cluster", "namespace", "node", "node_id"},
		),
		NodeReachability: prometheus.NewGaugeVec(
			prometheus.GaugeOpts{
				Name: "rqlite_node_reachable",
				Help: "Node reachability (1=reachable, 0=unreachable)",
			},
			[]string{"cluster", "namespace", "node", "node_id"},
		),
		RaftState: prometheus.NewGaugeVec(
			prometheus.GaugeOpts{
				Name: "rqlite_raft_state",
				Help: "Raft state (0=follower, 1=candidate, 2=leader)",
			},
			[]string{"cluster", "namespace", "node", "node_id", "state"},
		),

		QueryLatency: prometheus.NewHistogramVec(
			prometheus.HistogramOpts{
				Name:    "rqlite_query_duration_seconds",
				Help:    "Query execution time in seconds",
				Buckets: prometheus.DefBuckets,
			},
			[]string{"cluster", "namespace", "query_type"},
		),
		ReadLatency: prometheus.NewHistogramVec(
			prometheus.HistogramOpts{
				Name:    "rqlite_read_latency_seconds",
				Help:    "Read query latency in seconds for scaling decisions",
				Buckets: []float64{.001, .005, .01, .025, .05, .1, .25, .5, 1, 2.5, 5, 10},
			},
			[]string{"cluster", "namespace", "node"},
		),
		DatabaseSize: prometheus.NewGaugeVec(
			prometheus.GaugeOpts{
				Name: "rqlite_database_size_bytes",
				Help: "Database size in bytes",
			},
			[]string{"cluster", "namespace", "node"},
		),
		ConnectionCount: prometheus.NewGaugeVec(
			prometheus.GaugeOpts{
				Name: "rqlite_active_connections",
				Help: "Number of active connections",
			},
			[]string{"cluster", "namespace", "node"},
		),

		RaftIndex: prometheus.NewGaugeVec(
			prometheus.GaugeOpts{
				Name: "rqlite_raft_last_log_index",
				Help: "Last log index in raft",
			},
			[]string{"cluster", "namespace", "node"},
		),
		RaftTerm: prometheus.NewGaugeVec(
			prometheus.GaugeOpts{
				Name: "rqlite_raft_term",
				Help: "Current raft term",
			},
			[]string{"cluster", "namespace", "node"},
		),
		RaftCommitIndex: prometheus.NewGaugeVec(
			prometheus.GaugeOpts{
				Name: "rqlite_raft_commit_index",
				Help: "Raft commit index",
			},
			[]string{"cluster", "namespace", "node"},
		),
		RaftAppliedIndex: prometheus.NewGaugeVec(
			prometheus.GaugeOpts{
				Name: "rqlite_raft_applied_index",
				Help: "Raft applied index",
			},
			[]string{"cluster", "namespace", "node"},
		),

		DiskUsage: prometheus.NewGaugeVec(
			prometheus.GaugeOpts{
				Name: "rqlite_disk_usage_bytes",
				Help: "Disk usage in bytes",
			},
			[]string{"cluster", "namespace", "node"},
		),
		BackupCount: prometheus.NewGaugeVec(
			prometheus.GaugeOpts{
				Name: "rqlite_backup_count",
				Help: "Number of backups",
			},
			[]string{"cluster", "namespace"},
		),
		LastBackupAge: prometheus.NewGaugeVec(
			prometheus.GaugeOpts{
				Name: "rqlite_last_backup_age_seconds",
				Help: "Age of last backup in seconds",
			},
			[]string{"cluster", "namespace"},
		),

		HPAStatus: prometheus.NewGaugeVec(
			prometheus.GaugeOpts{
				Name: "rqlite_hpa_enabled",
				Help: "HPA status (1=enabled, 0=disabled)",
			},
			[]string{"cluster", "namespace"},
		),
		ScalingEvents: prometheus.NewCounterVec(
			prometheus.CounterOpts{
				Name: "rqlite_scaling_events_total",
				Help: "Total number of scaling events",
			},
			[]string{"cluster", "namespace", "direction"},
		),
		StorageExpansions: prometheus.NewCounterVec(
			prometheus.CounterOpts{
				Name: "rqlite_storage_expansions_total",
				Help: "Total number of storage expansions",
			},
			[]string{"cluster", "namespace"},
		),

		DBExecutions: prometheus.NewCounterVec(
			prometheus.CounterOpts{
				Name: "rqlite_db_executions_total",
				Help: "Total database executions counter",
			},
			[]string{"cluster", "namespace", "node"},
		),
		DBQueries: prometheus.NewCounterVec(
			prometheus.CounterOpts{
				Name: "rqlite_db_queries_total",
				Help: "Total queries processed counter",
			},
			[]string{"cluster", "namespace", "node"},
		),
		DBExecutionErrors: prometheus.NewCounterVec(
			prometheus.CounterOpts{
				Name: "rqlite_db_execution_errors_total",
				Help: "Execution errors counter",
			},
			[]string{"cluster", "namespace", "node"},
		),
		DBQueryErrors: prometheus.NewCounterVec(
			prometheus.CounterOpts{
				Name: "rqlite_db_query_errors_total",
				Help: "Query errors counter",
			},
			[]string{"cluster", "namespace", "node"},
		),

		RaftLastContact: prometheus.NewGaugeVec(
			prometheus.GaugeOpts{
				Name: "rqlite_raft_last_contact_seconds",
				Help: "Time since last leader contact",
			},
			[]string{"cluster", "namespace", "node"},
		),
		RaftLeaderChanges: prometheus.NewCounterVec(
			prometheus.CounterOpts{
				Name: "rqlite_raft_leader_changes_total",
				Help: "Leadership changes observed",
			},
			[]string{"cluster", "namespace"},
		),

		SQLiteDBSize: prometheus.NewGaugeVec(
			prometheus.GaugeOpts{
				Name: "rqlite_sqlite_db_size_bytes",
				Help: "Database file size",
			},
			[]string{"cluster", "namespace", "node"},
		),
		SQLiteWALSize: prometheus.NewGaugeVec(
			prometheus.GaugeOpts{
				Name: "rqlite_sqlite_wal_size_bytes",
				Help: "Write-Ahead Log size",
			},
			[]string{"cluster", "namespace", "node"},
		),

		SQLiteConnectionsOpen: prometheus.NewGaugeVec(
			prometheus.GaugeOpts{
				Name: "rqlite_sqlite_connections_open",
				Help: "Active database connections (ro/rw)",
			},
			[]string{"cluster", "namespace", "node", "type"},
		),
		SQLiteConnectionsIdle: prometheus.NewGaugeVec(
			prometheus.GaugeOpts{
				Name: "rqlite_sqlite_connections_idle",
				Help: "Idle database connections (ro/rw)",
			},
			[]string{"cluster", "namespace", "node", "type"},
		),
		SQLiteConnectionsWait: prometheus.NewCounterVec(
			prometheus.CounterOpts{
				Name: "rqlite_sqlite_connections_wait_count",
				Help: "Connection wait events",
			},
			[]string{"cluster", "namespace", "node", "type"},
		),

		BackupInfo: prometheus.NewGaugeVec(
			prometheus.GaugeOpts{
				Name: "rqlite_backup_info",
				Help: "Backup information with labels for name and creation time",
			},
			[]string{"cluster", "namespace", "backup_name", "creation_time", "backup_type"},
		),
	}

	metrics.Registry.MustRegister(
		m.ClusterHealth, m.ClusterSize, m.LeaderChanges, m.QuorumStatus,
		m.NodeStatus, m.NodeReachability, m.RaftState,
		m.QueryLatency, m.ReadLatency, m.DatabaseSize, m.ConnectionCount,
		m.RaftIndex, m.RaftTerm, m.RaftCommitIndex, m.RaftAppliedIndex,
		m.DiskUsage, m.BackupCount, m.LastBackupAge,
		m.HPAStatus, m.ScalingEvents, m.StorageExpansions,
		m.DBExecutions, m.DBQueries, m.DBExecutionErrors, m.DBQueryErrors,
		m.RaftLastContact, m.RaftLeaderChanges,
		m.SQLiteDBSize, m.SQLiteWALSize,
		m.SQLiteConnectionsOpen, m.SQLiteConnectionsIdle, m.SQLiteConnectionsWait,
		m.BackupInfo,
	)

	return &MetricsCollector{
		Client:  client,
		Metrics: m,
	}
}

func (mc *MetricsCollector) CollectMetrics(ctx context.Context, cluster *rqlitev1alpha1.RqliteCluster) error {
	logger := log.FromContext(ctx)

	logger.Info("Collecting metrics for cluster", "cluster", cluster.Name)

	if err := mc.collectClusterMetrics(ctx, cluster); err != nil {
		logger.Error(err, "Failed to collect cluster metrics")
	}

	if err := mc.collectNodeMetrics(ctx, cluster); err != nil {
		logger.Error(err, "Failed to collect node metrics")
	}

	if err := mc.collectEnhancedMetrics(ctx, cluster); err != nil {
		logger.Error(err, "Failed to collect enhanced metrics")
	}

	if err := mc.collectScalingMetrics(ctx, cluster); err != nil {
		logger.Error(err, "Failed to collect scaling metrics")
	}

	if err := mc.collectBackupMetrics(ctx, cluster); err != nil {
		logger.Error(err, "Failed to collect backup metrics")
	}

	return nil
}

func (mc *MetricsCollector) collectClusterMetrics(ctx context.Context, cluster *rqlitev1alpha1.RqliteCluster) error {
	labels := []string{cluster.Name, cluster.Namespace}

	mc.Metrics.ClusterSize.WithLabelValues(labels...).Set(float64(cluster.Spec.Size))

	var readyReplicas int32 = 0

	var sts appsv1.StatefulSet
	stsKey := client.ObjectKey{
		Name:      cluster.Name,
		Namespace: cluster.Namespace,
	}

	if err := mc.Client.Get(ctx, stsKey, &sts); err == nil {
		readyReplicas = sts.Status.ReadyReplicas
	}

	var healthScore float64
	if cluster.Spec.Size > 0 {
		healthScore = float64(readyReplicas) / float64(cluster.Spec.Size)
	}
	mc.Metrics.ClusterHealth.WithLabelValues(labels...).Set(healthScore)

	quorumSize := cluster.Spec.Size/2 + 1
	if readyReplicas >= quorumSize {
		mc.Metrics.QuorumStatus.WithLabelValues(labels...).Set(1)
	} else {
		mc.Metrics.QuorumStatus.WithLabelValues(labels...).Set(0)
	}

	return nil
}

func (mc *MetricsCollector) collectNodeMetrics(ctx context.Context, cluster *rqlitev1alpha1.RqliteCluster) error {
	podList := &corev1.PodList{}
	listOpts := []client.ListOption{
		client.InNamespace(cluster.Namespace),
		client.MatchingLabels{
			"app.kubernetes.io/instance": cluster.Name,
		},
	}

	if err := mc.Client.List(ctx, podList, listOpts...); err != nil {
		return fmt.Errorf("failed to list pods: %w", err)
	}

	for _, pod := range podList.Items {
		if pod.Status.Phase != corev1.PodRunning {
			continue
		}

		if err := mc.collectPodMetrics(ctx, cluster, &pod); err != nil {
			log.FromContext(ctx).Error(err, "Failed to collect pod metrics", "pod", pod.Name)
		}
	}

	return nil
}

func (mc *MetricsCollector) collectPodMetrics(ctx context.Context, cluster *rqlitev1alpha1.RqliteCluster, pod *corev1.Pod) error {
	statusURL := fmt.Sprintf("http://%s.%s-headless.%s.svc.cluster.local:4001/status", pod.Name, cluster.Name, cluster.Namespace)

	client := &http.Client{
		Timeout: 10 * time.Second,
	}

	resp, err := client.Get(statusURL)
	if err != nil {
		return fmt.Errorf("failed to get status from %s: %w", pod.Name, err)
	}
	defer func() {
		if err := resp.Body.Close(); err != nil {
		}
	}()

	if resp.StatusCode != http.StatusOK {
		return fmt.Errorf("status endpoint returned %d for %s", resp.StatusCode, pod.Name)
	}

	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return fmt.Errorf("failed to read response body: %w", err)
	}

	var status RqliteStatusResponse
	if err := json.Unmarshal(body, &status); err != nil {
		return fmt.Errorf("failed to unmarshal status response: %w", err)
	}

	labels := []string{cluster.Name, cluster.Namespace, pod.Name, status.Store.Leader.NodeID}

	mc.Metrics.NodeStatus.WithLabelValues(labels...).Set(1)

	raftStateValue := mc.getRaftStateValue(status.Store.RaftState)
	stateLabels := append(labels, status.Store.RaftState)
	mc.Metrics.RaftState.WithLabelValues(stateLabels...).Set(raftStateValue)

	mc.collectEnhancedStatusMetrics(cluster, pod, &status)

	if err := mc.collectNodesMetrics(ctx, cluster, pod); err != nil {
		log.FromContext(ctx).Error(err, "Failed to collect nodes metrics", "pod", pod.Name)
	}

	if err := mc.collectReadLatencyMetrics(ctx, cluster, pod); err != nil {
		log.FromContext(ctx).Error(err, "Failed to collect read latency metrics", "pod", pod.Name)
	}

	return nil
}

func (mc *MetricsCollector) collectNodesMetrics(_ context.Context, cluster *rqlitev1alpha1.RqliteCluster, pod *corev1.Pod) error {
	nodesURL := fmt.Sprintf("http://%s.%s-headless.%s.svc.cluster.local:4001/nodes", pod.Name, cluster.Name, cluster.Namespace)

	client := &http.Client{
		Timeout: 10 * time.Second,
	}

	resp, err := client.Get(nodesURL)
	if err != nil {
		return fmt.Errorf("failed to get nodes from %s: %w", pod.Name, err)
	}
	defer func() {
		if err := resp.Body.Close(); err != nil {
		}
	}()

	if resp.StatusCode != http.StatusOK {
		return fmt.Errorf("nodes endpoint returned %d for %s", resp.StatusCode, pod.Name)
	}

	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return fmt.Errorf("failed to read nodes response body: %w", err)
	}

	var nodes RqliteNodesResponse
	if err := json.Unmarshal(body, &nodes); err != nil {
		return fmt.Errorf("failed to unmarshal nodes response: %w", err)
	}

	for nodeID, nodeInfo := range nodes {
		labels := []string{cluster.Name, cluster.Namespace, nodeInfo.Addr, nodeID}

		if nodeInfo.Reachable {
			mc.Metrics.NodeReachability.WithLabelValues(labels...).Set(1)
		} else {
			mc.Metrics.NodeReachability.WithLabelValues(labels...).Set(0)
		}
	}

	return nil
}

func (mc *MetricsCollector) collectScalingMetrics(_ context.Context, cluster *rqlitev1alpha1.RqliteCluster) error {
	labels := []string{cluster.Name, cluster.Namespace}

	if cluster.Status.ScalingStatus != nil && cluster.Status.ScalingStatus.HPAEnabled {
		mc.Metrics.HPAStatus.WithLabelValues(labels...).Set(1)
	} else {
		mc.Metrics.HPAStatus.WithLabelValues(labels...).Set(0)
	}

	return nil
}

func (mc *MetricsCollector) collectBackupMetrics(ctx context.Context, cluster *rqlitev1alpha1.RqliteCluster) error {
	labels := []string{cluster.Name, cluster.Namespace}

	var continuousBackup rqlitev1alpha1.RqliteContinuousBackup
	err := mc.Client.Get(ctx, types.NamespacedName{
		Name:      cluster.Name + "-continuous-backup",
		Namespace: cluster.Namespace,
	}, &continuousBackup)

	if err == nil {
		mc.Metrics.BackupCount.WithLabelValues(labels...).Set(float64(continuousBackup.Status.BackupCount))

		if continuousBackup.Status.LastBackupTime != nil {
			age := time.Since(continuousBackup.Status.LastBackupTime.Time).Seconds()
			mc.Metrics.LastBackupAge.WithLabelValues(labels...).Set(age)
		}
	} else {
		mc.collectPVCBasedBackupMetrics(ctx, cluster)
	}

	return nil
}

func (mc *MetricsCollector) collectPVCBasedBackupMetrics(ctx context.Context, cluster *rqlitev1alpha1.RqliteCluster) {
	labels := []string{cluster.Name, cluster.Namespace}

	mc.Metrics.BackupInfo.Reset()

	var continuousBackup rqlitev1alpha1.RqliteContinuousBackup
	backupResourceName := cluster.Name + "-backup"
	err := mc.Client.Get(ctx, types.NamespacedName{
		Name:      backupResourceName,
		Namespace: cluster.Namespace,
	}, &continuousBackup)

	if err != nil {
		backupResourceName = cluster.Name + "-continuous-backup"
		err = mc.Client.Get(ctx, types.NamespacedName{
			Name:      backupResourceName,
			Namespace: cluster.Namespace,
		}, &continuousBackup)
	}

	if err == nil {
		mc.createBackupEntriesFromResource(cluster, &continuousBackup)
	} else {
		mc.Metrics.BackupCount.WithLabelValues(labels...).Set(0)
		mc.Metrics.LastBackupAge.WithLabelValues(labels...).Set(0)
	}
}

func (mc *MetricsCollector) createBackupEntriesFromResource(cluster *rqlitev1alpha1.RqliteCluster, continuousBackup *rqlitev1alpha1.RqliteContinuousBackup) {
	labels := []string{cluster.Name, cluster.Namespace}

	backupCount := int(continuousBackup.Status.BackupCount)

	if backupCount > 0 {
		interval := mc.parseInterval(continuousBackup.Spec.Interval)
		now := time.Now()

		maxEntries := 10
		if backupCount < maxEntries {
			maxEntries = backupCount
		}

		for i := 0; i < maxEntries; i++ {
			backupTime := now.Add(-time.Duration(i) * interval)
			backupName := fmt.Sprintf("%s-backup-%s", cluster.Name, backupTime.Format("20060102-150405"))
			creationTimeStr := backupTime.Format("2006-01-02T15:04:05Z")

			backupLabels := []string{
				cluster.Name,
				cluster.Namespace,
				backupName,
				creationTimeStr,
				"continuous",
			}
			mc.Metrics.BackupInfo.WithLabelValues(backupLabels...).Set(1)
		}

		mc.Metrics.BackupCount.WithLabelValues(labels...).Set(float64(backupCount))

		if continuousBackup.Status.LastBackupTime != nil {
			age := time.Since(continuousBackup.Status.LastBackupTime.Time).Seconds()
			mc.Metrics.LastBackupAge.WithLabelValues(labels...).Set(age)
		} else {
			mc.Metrics.LastBackupAge.WithLabelValues(labels...).Set(0)
		}
	} else {
		mc.Metrics.BackupCount.WithLabelValues(labels...).Set(0)
		mc.Metrics.LastBackupAge.WithLabelValues(labels...).Set(0)
	}
}

func (mc *MetricsCollector) parseInterval(interval string) time.Duration {
	duration, err := time.ParseDuration(interval)
	if err != nil {
		return 5 * time.Minute
	}
	return duration
}

func (mc *MetricsCollector) getRaftStateValue(state string) float64 {
	switch state {
	case RaftStateFollower:
		return 0
	case RaftStateCandidate:
		return 1
	case RaftStateLeader:
		return 2
	default:
		return -1
	}
}

func (mc *MetricsCollector) collectEnhancedMetrics(_ context.Context, cluster *rqlitev1alpha1.RqliteCluster) error {
	podList := &corev1.PodList{}
	listOpts := []client.ListOption{
		client.InNamespace(cluster.Namespace),
		client.MatchingLabels{
			"app.kubernetes.io/instance": cluster.Name,
		},
	}

	if err := mc.Client.List(context.Background(), podList, listOpts...); err != nil {
		return fmt.Errorf("failed to list pods: %w", err)
	}

	for _, pod := range podList.Items {
		if pod.Status.Phase != corev1.PodRunning {
			continue
		}

		if err := mc.collectDebugVarsMetrics(cluster, &pod); err != nil {
			continue
		}
	}

	return nil
}

func (mc *MetricsCollector) collectDebugVarsMetrics(cluster *rqlitev1alpha1.RqliteCluster, pod *corev1.Pod) error {
	debugURL := fmt.Sprintf("http://%s.%s-headless.%s.svc.cluster.local:4001/debug/vars", pod.Name, cluster.Name, cluster.Namespace)

	client := &http.Client{
		Timeout: 10 * time.Second,
	}

	resp, err := client.Get(debugURL)
	if err != nil {
		return fmt.Errorf("failed to get debug vars from %s: %w", pod.Name, err)
	}
	defer func() {
		if err := resp.Body.Close(); err != nil {
		}
	}()

	if resp.StatusCode != http.StatusOK {
		return fmt.Errorf("debug vars endpoint returned %d for %s", resp.StatusCode, pod.Name)
	}

	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return fmt.Errorf("failed to read debug vars response body: %w", err)
	}

	var debugVars RqliteDebugVarsResponse
	if err := json.Unmarshal(body, &debugVars); err != nil {
		return fmt.Errorf("failed to unmarshal debug vars response: %w", err)
	}

	labels := []string{cluster.Name, cluster.Namespace, pod.Name}

	mc.Metrics.DBExecutions.WithLabelValues(labels...).Add(float64(debugVars.DB.Executions))
	mc.Metrics.DBQueries.WithLabelValues(labels...).Add(float64(debugVars.DB.Queries))
	mc.Metrics.DBExecutionErrors.WithLabelValues(labels...).Add(float64(debugVars.DB.ExecutionErrors))
	mc.Metrics.DBQueryErrors.WithLabelValues(labels...).Add(float64(debugVars.DB.QueryErrors))

	clusterLabels := []string{cluster.Name, cluster.Namespace}
	mc.Metrics.RaftLeaderChanges.WithLabelValues(clusterLabels...).Add(float64(debugVars.Store.LeaderChanges))

	return nil
}

func (mc *MetricsCollector) collectEnhancedStatusMetrics(cluster *rqlitev1alpha1.RqliteCluster, pod *corev1.Pod, status *RqliteStatusResponse) {
	labels := []string{cluster.Name, cluster.Namespace, pod.Name}

	if status.Store.Raft.LastContact != nil {
		switch v := status.Store.Raft.LastContact.(type) {
		case string:
			if duration, err := time.ParseDuration(v); err == nil {
				contactSeconds := duration.Seconds()
				mc.Metrics.RaftLastContact.WithLabelValues(labels...).Set(contactSeconds)
			}
		case float64:
			mc.Metrics.RaftLastContact.WithLabelValues(labels...).Set(v)
		}
	}

	mc.Metrics.SQLiteDBSize.WithLabelValues(labels...).Set(float64(status.Store.SQLite3.DBSize))
	mc.Metrics.SQLiteWALSize.WithLabelValues(labels...).Set(float64(status.Store.SQLite3.WALSize))

	roLabels := append(labels, "readonly")
	mc.Metrics.SQLiteConnectionsOpen.WithLabelValues(roLabels...).Set(float64(status.Store.SQLite3.ConnPoolStats.ReadOnly.Open))
	mc.Metrics.SQLiteConnectionsIdle.WithLabelValues(roLabels...).Set(float64(status.Store.SQLite3.ConnPoolStats.ReadOnly.Idle))
	mc.Metrics.SQLiteConnectionsWait.WithLabelValues(roLabels...).Add(float64(status.Store.SQLite3.ConnPoolStats.ReadOnly.Wait))

	rwLabels := append(labels, "readwrite")
	mc.Metrics.SQLiteConnectionsOpen.WithLabelValues(rwLabels...).Set(float64(status.Store.SQLite3.ConnPoolStats.ReadWrite.Open))
	mc.Metrics.SQLiteConnectionsIdle.WithLabelValues(rwLabels...).Set(float64(status.Store.SQLite3.ConnPoolStats.ReadWrite.Idle))
	mc.Metrics.SQLiteConnectionsWait.WithLabelValues(rwLabels...).Add(float64(status.Store.SQLite3.ConnPoolStats.ReadWrite.Wait))
}

func (mc *MetricsCollector) collectReadLatencyMetrics(ctx context.Context, cluster *rqlitev1alpha1.RqliteCluster, pod *corev1.Pod) error {
	rqliteClient := NewRqliteClient(cluster.Namespace)

	latency, err := rqliteClient.ExecuteReadQuery(ctx, cluster.Name, pod.Name)

	labels := []string{cluster.Name, cluster.Namespace, pod.Name}
	mc.Metrics.ReadLatency.WithLabelValues(labels...).Observe(latency.Seconds())

	if err != nil {
		log.FromContext(ctx).V(1).Info("Read latency measurement failed", "pod", pod.Name, "error", err.Error())
	}

	return nil
}
