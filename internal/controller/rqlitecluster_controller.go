/*
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
*/

package controller

import (
	"context"
	"fmt"
	"time"

	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	policyv1 "k8s.io/api/policy/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/controller/controllerutil"
	"sigs.k8s.io/controller-runtime/pkg/log"

	rqlitev1alpha1 "github.com/mmiloszm/rqlite-operator/api/v1alpha1"
	"github.com/mmiloszm/rqlite-operator/internal/builders"
	rqliteclient "github.com/mmiloszm/rqlite-operator/internal/client"
	"github.com/mmiloszm/rqlite-operator/internal/managers"
	"github.com/mmiloszm/rqlite-operator/internal/utils"
)

const rqliteClusterFinalizer = "rqlite.example.com/finalizer"

type RqliteClusterReconciler struct {
	client.Client
	Scheme               *runtime.Scheme
	RollingUpdateManager *managers.RollingUpdateManager
	ScalingManager       *managers.ScalingManager
	MetricsManager       *managers.MetricsManager
	QuorumAwareScaler    *managers.QuorumAwareScaler
}

// +kubebuilder:rbac:groups=rqlite.example.com,resources=rqliteclusters,verbs=get;list;watch;create;update;patch;delete
// +kubebuilder:rbac:groups=rqlite.example.com,resources=rqliteclusters/status,verbs=get;update;patch
// +kubebuilder:rbac:groups=rqlite.example.com,resources=rqliteclusters/finalizers,verbs=update
// +kubebuilder:rbac:groups=apps,resources=statefulsets,verbs=get;list;watch;create;update;patch;delete
// +kubebuilder:rbac:groups=core,resources=services,verbs=get;list;watch;create;update;patch;delete
// +kubebuilder:rbac:groups=core,resources=pods,verbs=get;list;watch;create;update;patch;delete
// +kubebuilder:rbac:groups=core,resources=persistentvolumeclaims,verbs=get;list;watch;create;update;patch;delete
// +kubebuilder:rbac:groups=policy,resources=poddisruptionbudgets,verbs=get;list;watch;create;update;patch;delete
// +kubebuilder:rbac:groups=autoscaling,resources=horizontalpodautoscalers,verbs=get;list;watch;create;update;patch;delete

func (r *RqliteClusterReconciler) Reconcile(ctx context.Context, req ctrl.Request) (ctrl.Result, error) {
	log := log.FromContext(ctx)
	log.Info("Reconciliation loop started", "request", req.NamespacedName)

	var rqliteCluster rqlitev1alpha1.RqliteCluster
	if err := r.Get(ctx, req.NamespacedName, &rqliteCluster); err != nil {
		return ctrl.Result{}, client.IgnoreNotFound(err)
	}

	if _, ok := rqliteCluster.Annotations["rqlite.example.com/restore-in-progress"]; ok {
		log.Info("Restore is in progress for this cluster, skipping reconciliation to avoid conflicts.")
		return ctrl.Result{RequeueAfter: 30 * time.Second}, nil
	}

	isMarkedForDeletion := !rqliteCluster.ObjectMeta.DeletionTimestamp.IsZero()
	if isMarkedForDeletion {
		log.Info("RqliteCluster is marked for deletion")
		if controllerutil.ContainsFinalizer(&rqliteCluster, rqliteClusterFinalizer) {
			log.Info("Finalizer found, running cleanup logic")
			if err := r.finalizeRqliteCluster(ctx, &rqliteCluster); err != nil {
				log.Error(err, "Failed to finalize RqliteCluster")
				return ctrl.Result{}, err
			}
			controllerutil.RemoveFinalizer(&rqliteCluster, rqliteClusterFinalizer)
			if err := r.Update(ctx, &rqliteCluster); err != nil {
				log.Error(err, "Failed to remove finalizer")
				return ctrl.Result{}, err
			}
		}
		return ctrl.Result{}, nil
	}

	if !controllerutil.ContainsFinalizer(&rqliteCluster, rqliteClusterFinalizer) {
		log.Info("Adding finalizer to RqliteCluster")
		controllerutil.AddFinalizer(&rqliteCluster, rqliteClusterFinalizer)
		if err := r.Update(ctx, &rqliteCluster); err != nil {
			log.Error(err, "Failed to add finalizer")
			return ctrl.Result{}, err
		}
		return ctrl.Result{Requeue: true}, nil
	}

	log.Info("Reconciling Headless Service")

	desiredHeadlessSvc := builders.HeadlessServiceForRqliteCluster(&rqliteCluster)
	if err := ctrl.SetControllerReference(&rqliteCluster, desiredHeadlessSvc, r.Scheme); err != nil {
		log.Error(err, "Failed to set owner reference on StatefulSet")
		return ctrl.Result{}, err
	}

	foundHeadlessSvc := &corev1.Service{}
	err := r.Get(ctx, client.ObjectKey{Name: desiredHeadlessSvc.Name, Namespace: desiredHeadlessSvc.Namespace}, foundHeadlessSvc)

	if err != nil && apierrors.IsNotFound(err) {
		log.Info("Creating a new Headless Service", "Service.Namespace", desiredHeadlessSvc.Namespace, "Service.Name", desiredHeadlessSvc.Name)
		err = r.Create(ctx, desiredHeadlessSvc)
		if err != nil {
			log.Error(err, "Failed to create a new Headless Service", "Service.Namespace", desiredHeadlessSvc.Namespace, "Service.Name", desiredHeadlessSvc.Name)
			return ctrl.Result{}, err
		}
		log.Info("Headless Service created successfully, requeueing")
		return ctrl.Result{Requeue: true}, nil
	} else if err != nil {
		log.Error(err, "Failed to get Headless Service")
		return ctrl.Result{}, err
	}

	log.Info("Headless Service already exists", "Service.Name", foundHeadlessSvc.Name)

	log.Info("Reconciling Client Service")

	desiredClientSvc := builders.ClientServiceForRqliteCluster(&rqliteCluster)
	if err := ctrl.SetControllerReference(&rqliteCluster, desiredClientSvc, r.Scheme); err != nil {
		log.Error(err, "Failed to set owner reference on StatefulSet")
		return ctrl.Result{}, err
	}

	foundClientSvc := &corev1.Service{}
	err = r.Get(ctx, client.ObjectKey{Name: desiredClientSvc.Name, Namespace: desiredClientSvc.Namespace}, foundClientSvc)

	if err != nil && apierrors.IsNotFound(err) {
		log.Info("Creating a new Client Service", "Service.Namespace", desiredClientSvc.Namespace, "Service.Name", desiredClientSvc.Name)
		err = r.Create(ctx, desiredClientSvc)
		if err != nil {
			log.Error(err, "Failed to create a new Client Service", "Service.Namespace", desiredClientSvc.Namespace, "Service.Name", desiredClientSvc.Name)
			return ctrl.Result{}, err
		}
		log.Info("Client Service created successfully, requeueing")
		return ctrl.Result{Requeue: true}, nil
	} else if err != nil {
		log.Error(err, "Failed to get Client Service")
		return ctrl.Result{}, err
	}

	log.Info("Client Service already exists", "Service.Name", foundClientSvc.Name)

	log.Info("Reconciling StatefulSet")
	desiredSts, err := builders.StatefulSetForRqliteCluster(&rqliteCluster)
	if err != nil {
		log.Error(err, "Failed to define desired StatefulSet")
		return ctrl.Result{}, err
	}

	if err := ctrl.SetControllerReference(&rqliteCluster, desiredSts, r.Scheme); err != nil {
		log.Error(err, "Failed to set owner reference on StatefulSet")
		return ctrl.Result{}, err
	}

	foundSts := &appsv1.StatefulSet{}
	err = r.Get(ctx, client.ObjectKey{Name: desiredSts.Name, Namespace: desiredSts.Namespace}, foundSts)
	if err != nil {
		if apierrors.IsNotFound(err) {
			log.Info("Creating a new StatefulSet", "StatefulSet.Namespace", desiredSts.Namespace, "StatefulSet.Name", desiredSts.Name)
			if err = r.Create(ctx, desiredSts); err != nil {
				log.Error(err, "Failed to create new StatefulSet")
				return ctrl.Result{}, err
			}
			return ctrl.Result{Requeue: true}, nil
		}
		log.Error(err, "Failed to get StatefulSet")
		return ctrl.Result{}, err
	}

	log.Info("StatefulSet already exists, checking for updates")

	currentImage := foundSts.Spec.Template.Spec.Containers[0].Image
	desiredImage := desiredSts.Spec.Template.Spec.Containers[0].Image

	if currentImage != desiredImage {
		log.Info("Version update detected", "currentImage", currentImage, "desiredImage", desiredImage)

		currentVersion := managers.ExtractVersionFromImage(currentImage)
		targetVersion := managers.ExtractVersionFromImage(desiredImage)

		if err := utils.ValidateVersionUpdate(currentVersion, targetVersion); err != nil {
			log.Error(err, "Version update validation failed")
			if err := r.RollingUpdateManager.UpdateVersionValidationFailedStatus(ctx, &rqliteCluster, err.Error()); err != nil {
				log.Error(err, "Failed to update version validation status")
			}
			return ctrl.Result{RequeueAfter: 30 * time.Second}, nil
		}

		if result, err := r.RollingUpdateManager.HandleRollingUpdate(ctx, &rqliteCluster, foundSts, desiredSts); err != nil {
			log.Error(err, "Rolling update failed")
			return result, err
		} else if result.Requeue || result.RequeueAfter > 0 {
			return result, nil
		}
	}

	if r.ScalingManager != nil {
		if result, err := r.ScalingManager.ReconcileScaling(ctx, &rqliteCluster); err != nil {
			log.Error(err, "Auto-scaling reconciliation failed")
			return result, err
		} else if result.Requeue || result.RequeueAfter > 0 {
			log.Info("Auto-scaling requested requeue", "requeue", result.Requeue, "requeueAfter", result.RequeueAfter)
			return result, nil
		}
	}

	if r.MetricsManager != nil {
		if result, err := r.MetricsManager.ReconcileMetrics(ctx, &rqliteCluster); err != nil {
			log.Error(err, "Metrics collection failed")
		} else if result.Requeue || result.RequeueAfter > 0 {
			return result, nil
		}
	}

	hpaEnabled := rqliteCluster.Spec.AutoScaling != nil && rqliteCluster.Spec.AutoScaling.Enabled &&
		rqliteCluster.Spec.AutoScaling.HPA != nil && rqliteCluster.Spec.AutoScaling.HPA.Enabled

	if hpaEnabled {
		log.Info("HPA is enabled, skipping replica reconciliation to allow HPA to manage scaling")
	} else if *foundSts.Spec.Replicas != *desiredSts.Spec.Replicas {
		log.Info("StatefulSet replicas mismatch, validating scaling operation",
			"Current", *foundSts.Spec.Replicas, "Desired", *desiredSts.Spec.Replicas)

		if err := r.validateScalingOperation(ctx, &rqliteCluster, foundSts); err != nil {
			log.Error(err, "Scaling operation blocked")
			if err := r.updateScalingBlockedStatus(ctx, &rqliteCluster, err.Error()); err != nil {
				log.Error(err, "Failed to update scaling blocked status")
			}
			return ctrl.Result{RequeueAfter: 30 * time.Second}, nil
		}

		log.Info("Scaling validation passed, updating replicas")
		patch := client.MergeFrom(foundSts.DeepCopy())
		foundSts.Spec.Replicas = desiredSts.Spec.Replicas
		if err = r.Patch(ctx, foundSts, patch); err != nil {
			log.Error(err, "Failed to patch StatefulSet")
			return ctrl.Result{}, err
		}
		return ctrl.Result{Requeue: true}, nil
	}

	log.Info("Checking if cluster is ready for leader health monitoring")
	leaderHealthy := true
	leaderHealthError := ""

	if foundSts.Status.ReadyReplicas > 0 {
		log.Info("Checking rqlite leader health")
		if err := r.checkLeaderHealth(ctx, &rqliteCluster); err != nil {
			log.Info("Leader health check failed", "error", err.Error())
			leaderHealthy = false
			leaderHealthError = err.Error()
		}
	} else {
		log.Info("No ready replicas yet, skipping leader health check")
		leaderHealthy = false
		leaderHealthError = "Waiting for pods to be ready"
	}

	log.Info("Reconciling PodDisruptionBudget")

	desiredPdb := podDisruptionBudgetForRqliteCluster(&rqliteCluster)
	if err := ctrl.SetControllerReference(&rqliteCluster, desiredPdb, r.Scheme); err != nil {
		log.Error(err, "Failed to set owner reference on PodDisruptionBudget")
		return ctrl.Result{}, err
	}

	foundPdb := &policyv1.PodDisruptionBudget{}
	err = r.Get(ctx, client.ObjectKey{Name: desiredPdb.Name, Namespace: desiredPdb.Namespace}, foundPdb)

	if err != nil && apierrors.IsNotFound(err) {
		log.Info("Creating a new PodDisruptionBudget", "PodDisruptionBudget.Namespace", desiredPdb.Namespace, "PodDisruptionBudget.Name", desiredPdb.Name)
		if err = r.Create(ctx, desiredPdb); err != nil {
			log.Error(err, "Failed to create new PodDisruptionBudget")
			return ctrl.Result{}, err
		}
		return ctrl.Result{Requeue: true}, nil
	} else if err != nil {
		log.Error(err, "Failed to get PodDisruptionBudget")
		return ctrl.Result{}, err
	}

	log.Info("PodDisruptionBudget already exists", "PodDisruptionBudget.Name", foundPdb.Name)

	log.Info("Reconciling auto-scaling HPA")
	if err := r.QuorumAwareScaler.CreateOrUpdateHPA(ctx, &rqliteCluster); err != nil {
		log.Error(err, "Failed to reconcile HPA")
		return ctrl.Result{}, err
	}

	log.Info("Evaluating intelligent quorum-aware scaling")
	if err := r.QuorumAwareScaler.EvaluateIntelligentScaling(ctx, &rqliteCluster); err != nil {
		log.Error(err, "Failed to evaluate intelligent scaling")
		return ctrl.Result{}, err
	}

	log.Info("Updating RqliteCluster Status")
	if err := r.updateStatus(ctx, &rqliteCluster, foundSts, leaderHealthy, leaderHealthError); err != nil {
		log.Error(err, "Failed to update status, requeuing")
		return ctrl.Result{}, err
	}

	log.Info("Reconciliation loop finished successfully")
	return ctrl.Result{}, nil
}

func (r *RqliteClusterReconciler) updateStatus(ctx context.Context, cr *rqlitev1alpha1.RqliteCluster, sts *appsv1.StatefulSet, leaderHealthy bool, leaderHealthError string) error {
	log := log.FromContext(ctx).WithValues("RqliteCluster", cr.Name, "Action", "UpdateStatus")

	cr.Status.ObservedGeneration = cr.Generation
	if sts != nil {
		cr.Status.CurrentReplicas = sts.Status.CurrentReplicas
		cr.Status.ReadyReplicas = sts.Status.ReadyReplicas
	} else {
		cr.Status.CurrentReplicas = 0
		cr.Status.ReadyReplicas = 0
	}

	cr.Status.Leader = "Unknown"

	availableCondition := metav1.Condition{
		Type:               "Available",
		ObservedGeneration: cr.Generation,
		Status:             metav1.ConditionFalse,
		Reason:             "MinimumReplicasNotMet",
		Message:            fmt.Sprintf("Waiting for %d replicas to be ready", cr.Spec.Size),
	}

	if cr.Status.ReadyReplicas >= cr.Spec.Size {
		availableCondition.Status = metav1.ConditionTrue
		availableCondition.Reason = "MinimumReplicasMet"
		availableCondition.Message = fmt.Sprintf("Cluster has %d ready replicas", cr.Status.ReadyReplicas)
	}

	meta.SetStatusCondition(&cr.Status.Conditions, availableCondition)

	progressingCondition := metav1.Condition{
		Type:               "Progressing",
		ObservedGeneration: cr.Generation,
		Status:             metav1.ConditionTrue,
		Reason:             "Reconciling",
		Message:            "Operator is reconciling the cluster",
	}

	if sts == nil {
		progressingCondition.Reason = "Pending"
		progressingCondition.Message = "Waiting for StatefulSet to be created"
	} else if sts.Status.UpdateRevision != sts.Status.CurrentRevision {
		progressingCondition.Reason = "RollingUpdate"
		progressingCondition.Message = fmt.Sprintf("Rolling update to revision '%s' in progress: %d/%d replicas updated",
			sts.Status.UpdateRevision, sts.Status.UpdatedReplicas, cr.Spec.Size)
	} else if sts.Status.Replicas != cr.Spec.Size {
		progressingCondition.Reason = "Scaling"
		progressingCondition.Message = fmt.Sprintf("Scaling cluster to %d replicas: current count is %d",
			cr.Spec.Size, sts.Status.Replicas)
	} else if sts.Status.ReadyReplicas < cr.Spec.Size {
		progressingCondition.Reason = "WaitingForPods"
		progressingCondition.Message = fmt.Sprintf("Waiting for pods to be ready: %d/%d ready", sts.Status.ReadyReplicas, cr.Spec.Size)
	} else if availableCondition.Status == metav1.ConditionTrue {
		progressingCondition.Status = metav1.ConditionFalse
		progressingCondition.Reason = "Stable"
		progressingCondition.Message = "Cluster is stable and available"
	}

	meta.SetStatusCondition(&cr.Status.Conditions, progressingCondition)

	scalingAllowedCondition := metav1.Condition{
		Type:               "ScalingAllowed",
		ObservedGeneration: cr.Generation,
		Status:             metav1.ConditionTrue,
		Reason:             "ScalingPermitted",
		Message:            "Cluster is healthy and scaling operations are allowed",
		LastTransitionTime: metav1.Now(),
	}
	meta.SetStatusCondition(&cr.Status.Conditions, scalingAllowedCondition)

	var leaderCondition metav1.Condition
	if leaderHealthy {
		leaderCondition = metav1.Condition{
			Type:               "LeaderHealthy",
			ObservedGeneration: cr.Generation,
			Status:             metav1.ConditionTrue,
			Reason:             "LeaderResponsive",
			Message:            fmt.Sprintf("Leader node %s is healthy and responsive", cr.Status.LeaderID),
			LastTransitionTime: metav1.Now(),
		}
	} else {
		leaderCondition = metav1.Condition{
			Type:               "LeaderHealthy",
			ObservedGeneration: cr.Generation,
			Status:             metav1.ConditionFalse,
			Reason:             "LeaderUnreachable",
			Message:            fmt.Sprintf("Leader health check failed: %s", leaderHealthError),
			LastTransitionTime: metav1.Now(),
		}
	}
	meta.SetStatusCondition(&cr.Status.Conditions, leaderCondition)

	log.Info("Attempting to update RqliteCluster status")
	err := r.Status().Update(ctx, cr)
	if err != nil {
		log.Error(err, "Failed to update RqliteCluster status")
		return err
	}

	log.Info("Successfully updated RqliteCluster status")
	return nil
}

func (r *RqliteClusterReconciler) updateScalingBlockedStatus(ctx context.Context, cr *rqlitev1alpha1.RqliteCluster, reason string) error {
	log := log.FromContext(ctx).WithValues("RqliteCluster", cr.Name, "Action", "UpdateScalingStatus")

	scalingCondition := metav1.Condition{
		Type:               "ScalingAllowed",
		ObservedGeneration: cr.Generation,
		Status:             metav1.ConditionFalse,
		Reason:             "ScalingBlocked",
		Message:            reason,
		LastTransitionTime: metav1.Now(),
	}

	meta.SetStatusCondition(&cr.Status.Conditions, scalingCondition)

	if err := r.Status().Update(ctx, cr); err != nil {
		log.Error(err, "Failed to update scaling blocked status")
		return err
	}

	log.Info("Updated scaling blocked status", "reason", reason)
	return nil
}

func (r *RqliteClusterReconciler) finalizeRqliteCluster(ctx context.Context, cr *rqlitev1alpha1.RqliteCluster) error {
	log := log.FromContext(ctx).WithValues("RqliteCluster", cr.Name, "Action", "Finalize")

	log.Info("Starting cleanup logic for RqliteCluster")

	log.Info("Cleanup finished successfully")
	return nil
}

func (r *RqliteClusterReconciler) validateScalingOperation(ctx context.Context, cr *rqlitev1alpha1.RqliteCluster, currentSts *appsv1.StatefulSet) error {
	if _, ok := cr.Annotations["rqlite.example.com/scaling-override"]; ok {
		return nil
	}

	if _, ok := cr.Annotations["rqlite.example.com/restore-in-progress"]; ok {
		return fmt.Errorf("scaling blocked: restore operation is in progress")
	}

	currentSize := *currentSts.Spec.Replicas
	desiredSize := cr.Spec.Size

	if desiredSize > currentSize {
		return nil
	}

	if err := utils.ValidateQuorumRequirements(currentSize, desiredSize); err != nil {
		return err
	}

	if err := r.validateClusterHealth(ctx, cr, currentSts); err != nil {
		return fmt.Errorf("scaling blocked: %w", err)
	}

	return nil
}

func (r *RqliteClusterReconciler) validateClusterHealth(ctx context.Context, cr *rqlitev1alpha1.RqliteCluster, sts *appsv1.StatefulSet) error {
	if sts.Status.UpdateRevision != sts.Status.CurrentRevision {
		return fmt.Errorf("cluster is performing rolling update, wait for completion")
	}

	requiredReplicas := *sts.Spec.Replicas
	currentQuorum := (requiredReplicas / 2) + 1

	if sts.Status.ReadyReplicas < currentQuorum {
		return fmt.Errorf("cluster does not have quorum: %d/%d replicas ready (need %d)",
			sts.Status.ReadyReplicas, requiredReplicas, currentQuorum)
	}

	return nil
}

func (r *RqliteClusterReconciler) checkLeaderHealth(ctx context.Context, cr *rqlitev1alpha1.RqliteCluster) error {
	log := log.FromContext(ctx).WithValues("RqliteCluster", cr.Name, "Action", "CheckLeaderHealth")

	rqliteClient := rqliteclient.NewRqliteClient(cr.Namespace)
	status, err := rqliteClient.GetClusterStatus(ctx, cr.Name)
	if err != nil {
		log.Error(err, "Failed to get cluster status from rqlite API")
		return fmt.Errorf("failed to get cluster status: %w", err)
	}

	leaderID, leaderAddress := rqliteclient.GetLeaderInfo(status)

	previousLeader := cr.Status.LeaderID
	cr.Status.LeaderID = leaderID
	cr.Status.Leader = leaderAddress

	if previousLeader != "" && previousLeader != leaderID {
		log.Info("Leadership change detected", "previousLeader", previousLeader, "newLeader", leaderID)
		now := metav1.Now()
		cr.Status.LastLeaderChange = &now
	} else if cr.Status.LastLeaderChange == nil && leaderID != "" {
		now := metav1.Now()
		cr.Status.LastLeaderChange = &now
	}

	cr.Status.ClusterNodes = rqliteclient.ConvertToNodeStatuses(status)

	if leaderID == "" {
		return fmt.Errorf("no leader detected in cluster")
	}

	log.Info("Leader health check successful", "leaderID", leaderID, "leaderAddress", leaderAddress)
	return nil
}

func (r *RqliteClusterReconciler) SetupWithManager(mgr ctrl.Manager) error {
	r.RollingUpdateManager = managers.NewRollingUpdateManager(mgr.GetClient())

	r.ScalingManager = managers.NewScalingManager(mgr.GetClient())

	r.MetricsManager = managers.NewMetricsManager(mgr.GetClient())

	r.QuorumAwareScaler = managers.NewQuorumAwareScaler(mgr.GetClient())

	if err := mgr.Add(r.MetricsManager); err != nil {
		return err
	}

	return ctrl.NewControllerManagedBy(mgr).
		For(&rqlitev1alpha1.RqliteCluster{}).
		Named("rqlitecluster").
		Owns(&corev1.Service{}).
		Owns(&appsv1.StatefulSet{}).
		Complete(r)
}
