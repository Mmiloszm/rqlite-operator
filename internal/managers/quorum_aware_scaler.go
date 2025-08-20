package managers

import (
	"context"
	"fmt"

	rqlitev1alpha1 "github.com/mmiloszm/rqlite-operator/api/v1alpha1"
	appsv1 "k8s.io/api/apps/v1"
	autoscalingv2 "k8s.io/api/autoscaling/v2"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/log"
)

type QuorumAwareScaler struct {
	Client client.Client
}

func NewQuorumAwareScaler(client client.Client) *QuorumAwareScaler {
	return &QuorumAwareScaler{
		Client: client,
	}
}

func (q *QuorumAwareScaler) CreateOrUpdateHPA(ctx context.Context, cluster *rqlitev1alpha1.RqliteCluster) error {
	logger := log.FromContext(ctx)

	if cluster.Spec.AutoScaling == nil || !cluster.Spec.AutoScaling.Enabled {
		return q.cleanupHPA(ctx, cluster)
	}

	hpa := q.buildQuorumAwareHPA(cluster)

	existingHPA := &autoscalingv2.HorizontalPodAutoscaler{}
	err := q.Client.Get(ctx, types.NamespacedName{
		Name:      hpa.Name,
		Namespace: hpa.Namespace,
	}, existingHPA)

	if err != nil {
		if err := q.Client.Create(ctx, hpa); err != nil {
			return fmt.Errorf("failed to create HPA: %w", err)
		}
		logger.Info("Created quorum-aware HPA", "hpa", hpa.Name)
	} else {
		existingHPA.Spec = hpa.Spec
		if err := q.Client.Update(ctx, existingHPA); err != nil {
			return fmt.Errorf("failed to update HPA: %w", err)
		}
		logger.Info("Updated quorum-aware HPA", "hpa", hpa.Name)
	}

	return nil
}

func (q *QuorumAwareScaler) buildQuorumAwareHPA(cluster *rqlitev1alpha1.RqliteCluster) *autoscalingv2.HorizontalPodAutoscaler {
	autoScaling := cluster.Spec.AutoScaling

	minReplicas := q.ensureOdd(*autoScaling.HPA.MinReplicas)
	maxReplicas := q.ensureOdd(*autoScaling.HPA.MaxReplicas)

	if minReplicas < 3 {
		minReplicas = 3
	}

	hpa := &autoscalingv2.HorizontalPodAutoscaler{
		ObjectMeta: metav1.ObjectMeta{
			Name:      cluster.Name + "-hpa",
			Namespace: cluster.Namespace,
			Labels: map[string]string{
				"app.kubernetes.io/name":       "rqlite",
				"app.kubernetes.io/instance":   cluster.Name,
				"app.kubernetes.io/component":  "autoscaler",
				"app.kubernetes.io/managed-by": "rqlite-operator",
			},
		},
		Spec: autoscalingv2.HorizontalPodAutoscalerSpec{
			ScaleTargetRef: autoscalingv2.CrossVersionObjectReference{
				APIVersion: "apps/v1",
				Kind:       "StatefulSet",
				Name:       cluster.Name,
			},
			MinReplicas: &minReplicas,
			MaxReplicas: maxReplicas,
			Metrics:     q.buildMetrics(cluster),
			Behavior:    q.buildQuorumAwareBehavior(),
		},
	}

	return hpa
}

func (q *QuorumAwareScaler) buildMetrics(cluster *rqlitev1alpha1.RqliteCluster) []autoscalingv2.MetricSpec {
	autoScaling := cluster.Spec.AutoScaling
	var metrics []autoscalingv2.MetricSpec

	if autoScaling.HPA.CustomMetrics != nil {
		for _, customMetric := range autoScaling.HPA.CustomMetrics {
			switch customMetric.Type {
			case "External":
				metrics = append(metrics, autoscalingv2.MetricSpec{
					Type: autoscalingv2.ObjectMetricSourceType,
					Object: &autoscalingv2.ObjectMetricSource{
						DescribedObject: autoscalingv2.CrossVersionObjectReference{
							APIVersion: "rqlite.example.com/v1alpha1",
							Kind:       "RqliteCluster",
							Name:       cluster.Name,
						},
						Metric: autoscalingv2.MetricIdentifier{
							Name: customMetric.Name,
						},
						Target: autoscalingv2.MetricTarget{
							Type:  autoscalingv2.ValueMetricType,
							Value: &[]resource.Quantity{resource.MustParse(customMetric.TargetValue)}[0],
						},
					},
				})
			case "Resource":
				metrics = append(metrics, autoscalingv2.MetricSpec{
					Type: autoscalingv2.ResourceMetricSourceType,
					Resource: &autoscalingv2.ResourceMetricSource{
						Name: corev1.ResourceName(customMetric.Name),
						Target: autoscalingv2.MetricTarget{
							Type:         autoscalingv2.AverageValueMetricType,
							AverageValue: &[]resource.Quantity{resource.MustParse(customMetric.TargetValue)}[0],
						},
					},
				})
			case "Pods":
				metrics = append(metrics, autoscalingv2.MetricSpec{
					Type: autoscalingv2.PodsMetricSourceType,
					Pods: &autoscalingv2.PodsMetricSource{
						Metric: autoscalingv2.MetricIdentifier{
							Name: customMetric.Name,
						},
						Target: autoscalingv2.MetricTarget{
							Type:         autoscalingv2.AverageValueMetricType,
							AverageValue: &[]resource.Quantity{resource.MustParse(customMetric.TargetValue)}[0],
						},
					},
				})
			}
		}
	}

	if len(metrics) == 0 && autoScaling.HPA.TargetRequestsPerSecond != nil {
		targetRPS := *autoScaling.HPA.TargetRequestsPerSecond
		metrics = append(metrics, autoscalingv2.MetricSpec{
			Type: autoscalingv2.ObjectMetricSourceType,
			Object: &autoscalingv2.ObjectMetricSource{
				DescribedObject: autoscalingv2.CrossVersionObjectReference{
					APIVersion: "rqlite.example.com/v1alpha1",
					Kind:       "RqliteCluster",
					Name:       cluster.Name,
				},
				Metric: autoscalingv2.MetricIdentifier{
					Name: "rqlite_p95_read_latency_milliseconds",
				},
				Target: autoscalingv2.MetricTarget{
					Type:  autoscalingv2.ValueMetricType,
					Value: resource.NewQuantity(int64(targetRPS), resource.DecimalSI),
				},
			},
		})
	}

	if autoScaling.HPA.TargetCPUUtilizationPercentage != nil {
		cpuMetric := autoscalingv2.MetricSpec{
			Type: autoscalingv2.ResourceMetricSourceType,
			Resource: &autoscalingv2.ResourceMetricSource{
				Name: "cpu",
				Target: autoscalingv2.MetricTarget{
					Type:               autoscalingv2.UtilizationMetricType,
					AverageUtilization: autoScaling.HPA.TargetCPUUtilizationPercentage,
				},
			},
		}
		metrics = append(metrics, cpuMetric)
	}

	if autoScaling.HPA.TargetMemoryUtilizationPercentage != nil {
		memoryMetric := autoscalingv2.MetricSpec{
			Type: autoscalingv2.ResourceMetricSourceType,
			Resource: &autoscalingv2.ResourceMetricSource{
				Name: "memory",
				Target: autoscalingv2.MetricTarget{
					Type:               autoscalingv2.UtilizationMetricType,
					AverageUtilization: autoScaling.HPA.TargetMemoryUtilizationPercentage,
				},
			},
		}
		metrics = append(metrics, memoryMetric)
	}

	return metrics
}

func (q *QuorumAwareScaler) buildQuorumAwareBehavior() *autoscalingv2.HorizontalPodAutoscalerBehavior {
	return &autoscalingv2.HorizontalPodAutoscalerBehavior{
		ScaleUp: &autoscalingv2.HPAScalingRules{
			StabilizationWindowSeconds: &[]int32{60}[0],
			SelectPolicy:               &[]autoscalingv2.ScalingPolicySelect{autoscalingv2.MaxChangePolicySelect}[0],
			Policies: []autoscalingv2.HPAScalingPolicy{
				{
					Type:          autoscalingv2.PodsScalingPolicy,
					Value:         2,
					PeriodSeconds: 60,
				},
			},
		},
		ScaleDown: &autoscalingv2.HPAScalingRules{
			StabilizationWindowSeconds: &[]int32{300}[0],
			SelectPolicy:               &[]autoscalingv2.ScalingPolicySelect{autoscalingv2.MinChangePolicySelect}[0],
			Policies: []autoscalingv2.HPAScalingPolicy{
				{
					Type:          autoscalingv2.PodsScalingPolicy,
					Value:         2,
					PeriodSeconds: 120,
				},
			},
		},
	}
}

func (q *QuorumAwareScaler) ensureOdd(num int32) int32 {
	if num%2 == 0 {
		return num + 1
	}
	return num
}

func (q *QuorumAwareScaler) cleanupHPA(ctx context.Context, cluster *rqlitev1alpha1.RqliteCluster) error {
	hpa := &autoscalingv2.HorizontalPodAutoscaler{
		ObjectMeta: metav1.ObjectMeta{
			Name:      cluster.Name + "-hpa",
			Namespace: cluster.Namespace,
		},
	}

	err := q.Client.Delete(ctx, hpa)
	if err != nil && client.IgnoreNotFound(err) != nil {
		return fmt.Errorf("failed to delete HPA: %w", err)
	}

	return nil
}

func (q *QuorumAwareScaler) EvaluateIntelligentScaling(ctx context.Context, cluster *rqlitev1alpha1.RqliteCluster) error {
	logger := log.FromContext(ctx)

	if cluster.Spec.AutoScaling == nil || !cluster.Spec.AutoScaling.Enabled {
		return nil
	}

	sts := &appsv1.StatefulSet{}
	err := q.Client.Get(ctx, types.NamespacedName{
		Name:      cluster.Name,
		Namespace: cluster.Namespace,
	}, sts)
	if err != nil {
		logger.V(1).Info("Could not get StatefulSet for quorum check", "error", err)
		return nil
	}

	currentReplicas := int32(0)
	if sts.Spec.Replicas != nil {
		currentReplicas = *sts.Spec.Replicas
	}

	if currentReplicas > 0 && currentReplicas%2 == 0 {
		desiredReplicas := q.calculateQuorumAwareReplicas(currentReplicas, cluster)

		if desiredReplicas != currentReplicas {
			logger.Info("Adjusting replica count to maintain odd number for quorum",
				"cluster", cluster.Name,
				"current", currentReplicas,
				"desired", desiredReplicas,
				"reason", "maintaining Raft quorum (odd number required)")

			sts.Spec.Replicas = &desiredReplicas
			if err := q.Client.Update(ctx, sts); err != nil {
				return fmt.Errorf("failed to adjust replicas for quorum: %w", err)
			}
		}
	}

	return nil
}

func (q *QuorumAwareScaler) calculateQuorumAwareReplicas(current int32, cluster *rqlitev1alpha1.RqliteCluster) int32 {
	minReplicas := int32(3) // default
	if cluster.Spec.AutoScaling.HPA != nil && cluster.Spec.AutoScaling.HPA.MinReplicas != nil {
		minReplicas = *cluster.Spec.AutoScaling.HPA.MinReplicas
	}

	maxReplicas := int32(9) // default
	if cluster.Spec.AutoScaling.HPA != nil && cluster.Spec.AutoScaling.HPA.MaxReplicas != nil {
		maxReplicas = *cluster.Spec.AutoScaling.HPA.MaxReplicas
	}

	if minReplicas%2 == 0 {
		minReplicas++
	}
	if maxReplicas%2 == 0 {
		maxReplicas--
	}

	desired := current + 1
	if desired > maxReplicas {
		desired = current - 1
		if desired < minReplicas {
			desired = current
		}
	}

	return desired
}
