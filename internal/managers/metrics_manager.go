package managers

import (
	"context"
	"sync"
	"time"

	rqlitev1alpha1 "github.com/mmiloszm/rqlite-operator/api/v1alpha1"
	"github.com/mmiloszm/rqlite-operator/internal/client"
	ctrl "sigs.k8s.io/controller-runtime"
	ctrlclient "sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/log"
)

type MetricsManager struct {
	Client           ctrlclient.Client
	MetricsCollector *client.MetricsCollector
	collectInterval  time.Duration
	stopCh           chan struct{}
	mutex            sync.RWMutex
	isRunning        bool
}

func NewMetricsManager(cl ctrlclient.Client) *MetricsManager {
	return &MetricsManager{
		Client:           cl,
		MetricsCollector: client.NewMetricsCollector(cl),
		collectInterval:  30 * time.Second,
		stopCh:           make(chan struct{}),
	}
}

func (mm *MetricsManager) Start(ctx context.Context) error {
	logger := log.FromContext(ctx)

	mm.mutex.Lock()
	if mm.isRunning {
		mm.mutex.Unlock()
		return nil
	}
	mm.isRunning = true
	mm.mutex.Unlock()

	logger.Info("Starting metrics collection", "interval", mm.collectInterval)

	go mm.metricsCollectionLoop(ctx)

	return nil
}

func (mm *MetricsManager) Stop() {
	mm.mutex.Lock()
	defer mm.mutex.Unlock()

	if !mm.isRunning {
		return
	}

	close(mm.stopCh)
	mm.isRunning = false
}

func (mm *MetricsManager) SetCollectionInterval(interval time.Duration) {
	mm.mutex.Lock()
	defer mm.mutex.Unlock()
	mm.collectInterval = interval
}

func (mm *MetricsManager) metricsCollectionLoop(ctx context.Context) {
	logger := log.FromContext(ctx)
	ticker := time.NewTicker(mm.collectInterval)
	defer ticker.Stop()

	mm.collectAllClusters(ctx)

	for {
		select {
		case <-mm.stopCh:
			logger.Info("Stopping metrics collection")
			return
		case <-ticker.C:
			mm.mutex.RLock()
			currentInterval := mm.collectInterval
			mm.mutex.RUnlock()

			if ticker.C != time.NewTicker(currentInterval).C {
				ticker.Stop()
				ticker = time.NewTicker(currentInterval)
			}

			mm.collectAllClusters(ctx)
		case <-ctx.Done():
			logger.Info("Context cancelled, stopping metrics collection")
			return
		}
	}
}

func (mm *MetricsManager) collectAllClusters(ctx context.Context) {
	logger := log.FromContext(ctx)

	var clusterList rqlitev1alpha1.RqliteClusterList
	if err := mm.Client.List(ctx, &clusterList); err != nil {
		logger.Error(err, "Failed to list RqliteCluster resources")
		return
	}

	logger.V(1).Info("Collecting metrics", "clusters", len(clusterList.Items))

	for _, cluster := range clusterList.Items {
		if err := mm.MetricsCollector.CollectMetrics(ctx, &cluster); err != nil {
			logger.Error(err, "Failed to collect metrics for cluster", "cluster", cluster.Name)
		}
	}
}

func (mm *MetricsManager) ReconcileMetrics(ctx context.Context, cluster *rqlitev1alpha1.RqliteCluster) (ctrl.Result, error) {
	// This can be called during reconciliation to update metrics immediately
	// for important events like leader changes, scaling events, etc.

	if err := mm.MetricsCollector.CollectMetrics(ctx, cluster); err != nil {
		log.FromContext(ctx).Error(err, "Failed to collect metrics during reconciliation")
	}

	return ctrl.Result{}, nil
}

func (mm *MetricsManager) RecordLeaderChange(cluster *rqlitev1alpha1.RqliteCluster) {
	labels := []string{cluster.Name, cluster.Namespace}
	mm.MetricsCollector.Metrics.LeaderChanges.WithLabelValues(labels...).Inc()
}

func (mm *MetricsManager) RecordScalingEvent(cluster *rqlitev1alpha1.RqliteCluster, direction string) {
	labels := []string{cluster.Name, cluster.Namespace, direction}
	mm.MetricsCollector.Metrics.ScalingEvents.WithLabelValues(labels...).Inc()
}

func (mm *MetricsManager) RecordStorageExpansion(cluster *rqlitev1alpha1.RqliteCluster) {
	labels := []string{cluster.Name, cluster.Namespace}
	mm.MetricsCollector.Metrics.StorageExpansions.WithLabelValues(labels...).Inc()
}

func (mm *MetricsManager) UpdateQueryLatency(cluster *rqlitev1alpha1.RqliteCluster, queryType string, duration time.Duration) {
	labels := []string{cluster.Name, cluster.Namespace, queryType}
	mm.MetricsCollector.Metrics.QueryLatency.WithLabelValues(labels...).Observe(duration.Seconds())
}

func (mm *MetricsManager) IsRunning() bool {
	mm.mutex.RLock()
	defer mm.mutex.RUnlock()
	return mm.isRunning
}
