package managers

import (
	"context"
	"fmt"
	"time"

	rqlitev1alpha1 "github.com/mmiloszm/rqlite-operator/api/v1alpha1"
	autoscalingv2 "k8s.io/api/autoscaling/v2"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/log"
)

type ScalingManager struct {
	Client client.Client
}

func NewScalingManager(client client.Client) *ScalingManager {
	return &ScalingManager{
		Client: client,
	}
}

func (sm *ScalingManager) ReconcileScaling(ctx context.Context, cr *rqlitev1alpha1.RqliteCluster) (ctrl.Result, error) {
	logger := log.FromContext(ctx)

	if cr.Status.ScalingStatus == nil {
		cr.Status.ScalingStatus = &rqlitev1alpha1.ScalingStatus{}
	}

	var result ctrl.Result
	var requeue bool

	if cr.Spec.AutoScaling != nil && cr.Spec.AutoScaling.HPA != nil && cr.Spec.AutoScaling.HPA.Enabled {
		hpaResult, err := sm.reconcileHPA(ctx, cr)
		if err != nil {
			logger.Error(err, "Failed to reconcile HPA")
			return ctrl.Result{}, err
		}
		if hpaResult.Requeue || hpaResult.RequeueAfter > 0 {
			requeue = true
			if hpaResult.RequeueAfter > result.RequeueAfter {
				result.RequeueAfter = hpaResult.RequeueAfter
			}
		}
		cr.Status.ScalingStatus.HPAEnabled = true
	} else {
		if err := sm.deleteHPA(ctx, cr); err != nil {
			logger.Error(err, "Failed to delete HPA")
			return ctrl.Result{}, err
		}
		cr.Status.ScalingStatus.HPAEnabled = false
	}

	if cr.Spec.AutoScaling != nil && cr.Spec.AutoScaling.StorageExpansion != nil && cr.Spec.AutoScaling.StorageExpansion.Enabled {
		storageResult, err := sm.reconcileStorageExpansion(ctx, cr)
		if err != nil {
			logger.Error(err, "Failed to reconcile storage expansion")
			return ctrl.Result{}, err
		}
		if storageResult.Requeue || storageResult.RequeueAfter > 0 {
			requeue = true
			if storageResult.RequeueAfter > result.RequeueAfter {
				result.RequeueAfter = storageResult.RequeueAfter
			}
		}
		cr.Status.ScalingStatus.StorageExpansionEnabled = true
	} else {
		cr.Status.ScalingStatus.StorageExpansionEnabled = false
	}

	if requeue && result.RequeueAfter == 0 {
		result.Requeue = true
	}

	return result, nil
}

func (sm *ScalingManager) reconcileHPA(ctx context.Context, cr *rqlitev1alpha1.RqliteCluster) (ctrl.Result, error) {
	logger := log.FromContext(ctx)

	hpaName := cr.Name + "-hpa"
	hpa := &autoscalingv2.HorizontalPodAutoscaler{}

	err := sm.Client.Get(ctx, types.NamespacedName{
		Name:      hpaName,
		Namespace: cr.Namespace,
	}, hpa)

	if errors.IsNotFound(err) {
		hpa = sm.buildHPA(cr)
		logger.Info("Creating HPA", "name", hpaName)
		if err := sm.Client.Create(ctx, hpa); err != nil {
			return ctrl.Result{}, fmt.Errorf("failed to create HPA: %w", err)
		}
		return ctrl.Result{RequeueAfter: time.Second * 30}, nil
	} else if err != nil {
		return ctrl.Result{}, fmt.Errorf("failed to get HPA: %w", err)
	}

	desiredHPA := sm.buildHPA(cr)
	if !sm.hpaSpecsEqual(hpa, desiredHPA) {
		logger.Info("Updating HPA", "name", hpaName)
		hpa.Spec = desiredHPA.Spec
		if err := sm.Client.Update(ctx, hpa); err != nil {
			return ctrl.Result{}, fmt.Errorf("failed to update HPA: %w", err)
		}
	}

	return ctrl.Result{}, nil
}

func (sm *ScalingManager) buildHPA(cr *rqlitev1alpha1.RqliteCluster) *autoscalingv2.HorizontalPodAutoscaler {
	hpaSpec := cr.Spec.AutoScaling.HPA

	minReplicas := int32(3)
	if hpaSpec.MinReplicas != nil {
		minReplicas = *hpaSpec.MinReplicas
	}

	maxReplicas := int32(10)
	if hpaSpec.MaxReplicas != nil {
		maxReplicas = *hpaSpec.MaxReplicas
	}

	if minReplicas%2 == 0 {
		minReplicas++
	}

	var metrics []autoscalingv2.MetricSpec

	if hpaSpec.TargetCPUUtilizationPercentage != nil {
		metrics = append(metrics, autoscalingv2.MetricSpec{
			Type: autoscalingv2.ResourceMetricSourceType,
			Resource: &autoscalingv2.ResourceMetricSource{
				Name: corev1.ResourceCPU,
				Target: autoscalingv2.MetricTarget{
					Type:               autoscalingv2.UtilizationMetricType,
					AverageUtilization: hpaSpec.TargetCPUUtilizationPercentage,
				},
			},
		})
	}

	if hpaSpec.TargetMemoryUtilizationPercentage != nil {
		metrics = append(metrics, autoscalingv2.MetricSpec{
			Type: autoscalingv2.ResourceMetricSourceType,
			Resource: &autoscalingv2.ResourceMetricSource{
				Name: corev1.ResourceMemory,
				Target: autoscalingv2.MetricTarget{
					Type:               autoscalingv2.UtilizationMetricType,
					AverageUtilization: hpaSpec.TargetMemoryUtilizationPercentage,
				},
			},
		})
	}

	if hpaSpec.CustomMetrics != nil {
		for _, customMetric := range hpaSpec.CustomMetrics {
			switch customMetric.Type {
			case "External":
				metrics = append(metrics, autoscalingv2.MetricSpec{
					Type: autoscalingv2.ObjectMetricSourceType,
					Object: &autoscalingv2.ObjectMetricSource{
						DescribedObject: autoscalingv2.CrossVersionObjectReference{
							APIVersion: "rqlite.example.com/v1alpha1",
							Kind:       "RqliteCluster",
							Name:       cr.Name,
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
			case "Object":
				metrics = append(metrics, autoscalingv2.MetricSpec{
					Type: autoscalingv2.ObjectMetricSourceType,
					Object: &autoscalingv2.ObjectMetricSource{
						DescribedObject: autoscalingv2.CrossVersionObjectReference{
							APIVersion: "rqlite.example.com/v1alpha1",
							Kind:       "RqliteCluster",
							Name:       cr.Name,
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
			}
		}
	}

	hpa := &autoscalingv2.HorizontalPodAutoscaler{
		ObjectMeta: metav1.ObjectMeta{
			Name:      cr.Name + "-hpa",
			Namespace: cr.Namespace,
			Labels: map[string]string{
				"app.kubernetes.io/name":       "rqlite-hpa",
				"app.kubernetes.io/instance":   cr.Name,
				"app.kubernetes.io/component":  "autoscaler",
				"app.kubernetes.io/part-of":    "rqlite-cluster",
				"app.kubernetes.io/managed-by": "rqlite-operator",
			},
		},
		Spec: autoscalingv2.HorizontalPodAutoscalerSpec{
			ScaleTargetRef: autoscalingv2.CrossVersionObjectReference{
				APIVersion: "apps/v1",
				Kind:       "StatefulSet",
				Name:       cr.Name,
			},
			MinReplicas: &minReplicas,
			MaxReplicas: maxReplicas,
			Metrics:     metrics,
		},
	}

	if hpaSpec.ScaleBehavior != nil {
		hpa.Spec.Behavior = sm.buildScalingBehavior(hpaSpec.ScaleBehavior)
	}

	ctrl.SetControllerReference(cr, hpa, sm.Client.Scheme())

	return hpa
}

func (sm *ScalingManager) buildScalingBehavior(behaviorSpec *rqlitev1alpha1.ScaleBehaviorSpec) *autoscalingv2.HorizontalPodAutoscalerBehavior {
	behavior := &autoscalingv2.HorizontalPodAutoscalerBehavior{}

	if behaviorSpec.ScaleUp != nil {
		behavior.ScaleUp = sm.buildScalingPolicy(behaviorSpec.ScaleUp)
	}

	if behaviorSpec.ScaleDown != nil {
		behavior.ScaleDown = sm.buildScalingPolicy(behaviorSpec.ScaleDown)
	}

	return behavior
}

func (sm *ScalingManager) buildScalingPolicy(policySpec *rqlitev1alpha1.ScalingPolicySpec) *autoscalingv2.HPAScalingRules {
	rules := &autoscalingv2.HPAScalingRules{}

	if policySpec.StabilizationWindowSeconds != nil {
		rules.StabilizationWindowSeconds = policySpec.StabilizationWindowSeconds
	}

	selectPolicy := autoscalingv2.MaxChangePolicySelect
	if policySpec.SelectPolicy != "" {
		switch policySpec.SelectPolicy {
		case "Min":
			selectPolicy = autoscalingv2.MinChangePolicySelect
		case "Max":
			selectPolicy = autoscalingv2.MaxChangePolicySelect
		case "Disabled":
			selectPolicy = autoscalingv2.DisabledPolicySelect
		}
	}
	rules.SelectPolicy = &selectPolicy

	for _, policyRule := range policySpec.Policies {
		periodSeconds := int32(60)
		if policyRule.PeriodSeconds != nil {
			periodSeconds = *policyRule.PeriodSeconds
		}

		policy := autoscalingv2.HPAScalingPolicy{
			Type:          autoscalingv2.HPAScalingPolicyType(policyRule.Type),
			Value:         policyRule.Value,
			PeriodSeconds: periodSeconds,
		}
		rules.Policies = append(rules.Policies, policy)
	}

	return rules
}

func (sm *ScalingManager) hpaSpecsEqual(current, desired *autoscalingv2.HorizontalPodAutoscaler) bool {
	// Compare key fields
	if current.Spec.MinReplicas == nil || desired.Spec.MinReplicas == nil {
		return false
	}

	return *current.Spec.MinReplicas == *desired.Spec.MinReplicas &&
		current.Spec.MaxReplicas == desired.Spec.MaxReplicas &&
		len(current.Spec.Metrics) == len(desired.Spec.Metrics)
}

func (sm *ScalingManager) deleteHPA(ctx context.Context, cr *rqlitev1alpha1.RqliteCluster) error {
	hpaName := cr.Name + "-hpa"
	hpa := &autoscalingv2.HorizontalPodAutoscaler{}

	err := sm.Client.Get(ctx, types.NamespacedName{
		Name:      hpaName,
		Namespace: cr.Namespace,
	}, hpa)

	if errors.IsNotFound(err) {
		return nil
	} else if err != nil {
		return fmt.Errorf("failed to get HPA for deletion: %w", err)
	}

	if err := sm.Client.Delete(ctx, hpa); err != nil {
		return fmt.Errorf("failed to delete HPA: %w", err)
	}

	return nil
}

func (sm *ScalingManager) reconcileStorageExpansion(ctx context.Context, cr *rqlitev1alpha1.RqliteCluster) (ctrl.Result, error) {
	logger := log.FromContext(ctx)

	storageSpec := cr.Spec.AutoScaling.StorageExpansion
	checkInterval := time.Duration(300) * time.Second
	if storageSpec.CheckIntervalSeconds != nil {
		checkInterval = time.Duration(*storageSpec.CheckIntervalSeconds) * time.Second
	}

	pvcList := &corev1.PersistentVolumeClaimList{}
	listOpts := []client.ListOption{
		client.InNamespace(cr.Namespace),
		client.MatchingLabels{
			"app.kubernetes.io/instance": cr.Name,
		},
	}

	if err := sm.Client.List(ctx, pvcList, listOpts...); err != nil {
		return ctrl.Result{}, fmt.Errorf("failed to list PVCs: %w", err)
	}

	expansionPercentage := int32(50)
	if storageSpec.ExpansionPercentage != nil {
		expansionPercentage = *storageSpec.ExpansionPercentage
	}

	for _, pvc := range pvcList.Items {
		if !sm.storageClassSupportsExpansion(ctx, pvc.Spec.StorageClassName) {
			logger.Info("StorageClass does not support volume expansion",
				"pvc", pvc.Name,
				"storageClass", *pvc.Spec.StorageClassName)
			continue
		}

		if sm.shouldExpandStorage(cr, &pvc) {
			if err := sm.expandPVC(ctx, &pvc, expansionPercentage, storageSpec.MaxCapacity); err != nil {
				logger.Error(err, "Failed to expand PVC", "pvc", pvc.Name)
				continue
			}

			now := metav1.Now()
			cr.Status.ScalingStatus.LastStorageExpansion = &now
		}
	}

	return ctrl.Result{RequeueAfter: checkInterval}, nil
}

func (sm *ScalingManager) storageClassSupportsExpansion(ctx context.Context, storageClassName *string) bool {
	if storageClassName == nil {
		return false
	}

	supportedClasses := []string{"local-path", "gp2", "gp3", "standard", "fast"}
	for _, supported := range supportedClasses {
		if *storageClassName == supported {
			return true
		}
	}

	return false
}

func (sm *ScalingManager) shouldExpandStorage(cr *rqlitev1alpha1.RqliteCluster, pvc *corev1.PersistentVolumeClaim) bool {
	currentCapacity := pvc.Spec.Resources.Requests[corev1.ResourceStorage]
	currentBytes, ok := currentCapacity.AsInt64()
	if !ok {
		return false
	}

	tenGiBytes := int64(10 * 1024 * 1024 * 1024)
	if currentBytes > tenGiBytes {
		return false
	}

	if cr.Spec.Size > 5 {
		oneGiBytes := int64(1024 * 1024 * 1024)
		if currentBytes <= oneGiBytes {
			return true
		}
	}

	return false
}

func (sm *ScalingManager) expandPVC(ctx context.Context, pvc *corev1.PersistentVolumeClaim, expansionPercentage int32, maxCapacity string) error {
	logger := log.FromContext(ctx)

	currentCapacity := pvc.Spec.Resources.Requests[corev1.ResourceStorage]
	currentBytes, ok := currentCapacity.AsInt64()
	if !ok {
		return fmt.Errorf("failed to parse current storage capacity")
	}

	expansionBytes := currentBytes * int64(expansionPercentage) / 100
	newBytes := currentBytes + expansionBytes
	newCapacity := resource.NewQuantity(newBytes, resource.BinarySI)

	if maxCapacity != "" {
		maxCap := resource.MustParse(maxCapacity)
		if newCapacity.Cmp(maxCap) > 0 {
			logger.Info("Storage expansion would exceed max capacity",
				"pvc", pvc.Name,
				"newSize", newCapacity.String(),
				"maxCapacity", maxCapacity)
			return nil
		}
	}

	pvc.Spec.Resources.Requests[corev1.ResourceStorage] = *newCapacity

	logger.Info("Expanding PVC storage",
		"pvc", pvc.Name,
		"oldSize", currentCapacity.String(),
		"newSize", newCapacity.String())

	if err := sm.Client.Update(ctx, pvc); err != nil {
		return fmt.Errorf("failed to update PVC with expanded storage: %w", err)
	}

	return nil
}
