package managers

import (
	"context"
	"fmt"
	"strings"
	"time"

	rqlitev1alpha1 "github.com/mmiloszm/rqlite-operator/api/v1alpha1"
	"github.com/mmiloszm/rqlite-operator/internal/utils"
	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/log"
)

type RollingUpdateManager struct {
	client.Client
}

func NewRollingUpdateManager(client client.Client) *RollingUpdateManager {
	return &RollingUpdateManager{Client: client}
}

func ExtractVersionFromImage(image string) string {
	parts := strings.Split(image, ":")
	if len(parts) < 2 {
		return "latest"
	}
	return parts[len(parts)-1]
}

func (rum *RollingUpdateManager) HandleRollingUpdate(ctx context.Context, cr *rqlitev1alpha1.RqliteCluster, currentSts, desiredSts *appsv1.StatefulSet) (ctrl.Result, error) {
	log := log.FromContext(ctx).WithValues("RqliteCluster", cr.Name, "Action", "RollingUpdate")

	currentVersion := ExtractVersionFromImage(currentSts.Spec.Template.Spec.Containers[0].Image)
	targetVersion := ExtractVersionFromImage(desiredSts.Spec.Template.Spec.Containers[0].Image)

	if cr.Status.UpdateStatus == nil {
		cr.Status.UpdateStatus = &rqlitev1alpha1.UpdateStatus{
			Phase:           "Pending",
			CurrentVersion:  currentVersion,
			TargetVersion:   targetVersion,
			UpdatedReplicas: 0,
			TotalReplicas:   cr.Spec.Size,
			StartTime:       &metav1.Time{Time: time.Now()},
		}
		log.Info("Initializing rolling update", "from", currentVersion, "to", targetVersion)
	}

	if rum.shouldRollback(ctx, cr, currentSts) {
		return rum.performRollback(ctx, cr, currentSts, currentVersion)
	}

	if err := rum.validateUpdatePreconditions(ctx, cr, currentSts); err != nil {
		log.Info("Rolling update preconditions not met, waiting", "reason", err.Error())
		cr.Status.UpdateStatus.Phase = "Paused"
		cr.Status.UpdateStatus.LastError = err.Error()
		if err := rum.Status().Update(ctx, cr); err != nil {
			log.Error(err, "Failed to update status")
		}
		return ctrl.Result{RequeueAfter: 30 * time.Second}, nil
	}

	updateStrategy := rum.getUpdateStrategy(cr)

	if updateStrategy.LeaderUpdatePolicy == "LeaderLast" {
		return rum.performLeaderLastUpdate(ctx, cr, currentSts, desiredSts)
	}

	return rum.performStandardRollingUpdate(ctx, cr, currentSts, desiredSts)
}

func (rum *RollingUpdateManager) getUpdateStrategy(cr *rqlitev1alpha1.RqliteCluster) *rqlitev1alpha1.RollingUpdateSpec {
	if cr.Spec.UpdateStrategy == nil || cr.Spec.UpdateStrategy.RollingUpdate == nil {
		maxUnavailable := int32(1)
		timeout := int32(300)
		return &rqlitev1alpha1.RollingUpdateSpec{
			MaxUnavailable:     &maxUnavailable,
			LeaderUpdatePolicy: "LeaderLast",
			UpdateTimeout:      &timeout,
			RollbackOnFailure:  true,
		}
	}
	return cr.Spec.UpdateStrategy.RollingUpdate
}

func (rum *RollingUpdateManager) validateUpdatePreconditions(ctx context.Context, cr *rqlitev1alpha1.RqliteCluster, sts *appsv1.StatefulSet) error {
	requiredQuorum := (cr.Spec.Size / 2) + 1
	if sts.Status.ReadyReplicas < requiredQuorum {
		return fmt.Errorf("cluster does not have quorum: %d/%d replicas ready", sts.Status.ReadyReplicas, cr.Spec.Size)
	}

	if sts.Status.UpdateRevision != sts.Status.CurrentRevision {
		return nil
	}

	return nil
}

func (rum *RollingUpdateManager) performStandardRollingUpdate(ctx context.Context, cr *rqlitev1alpha1.RqliteCluster, currentSts, desiredSts *appsv1.StatefulSet) (ctrl.Result, error) {
	log := log.FromContext(ctx).WithValues("RqliteCluster", cr.Name, "Action", "StandardRollingUpdate")

	patch := client.MergeFrom(currentSts.DeepCopy())
	currentSts.Spec.Template.Spec.Containers[0].Image = desiredSts.Spec.Template.Spec.Containers[0].Image
	currentSts.Spec.UpdateStrategy = desiredSts.Spec.UpdateStrategy

	if err := rum.Patch(ctx, currentSts, patch); err != nil {
		log.Error(err, "Failed to patch StatefulSet for rolling update")
		cr.Status.UpdateStatus.Phase = "Failed"
		cr.Status.UpdateStatus.LastError = err.Error()
		if err := rum.Status().Update(ctx, cr); err != nil {
			log.Error(err, "Failed to update status")
		}
		return ctrl.Result{}, err
	}

	cr.Status.UpdateStatus.Phase = "InProgress"
	log.Info("Rolling update started")

	if err := rum.Status().Update(ctx, cr); err != nil {
		log.Error(err, "Failed to update status")
	}

	return ctrl.Result{RequeueAfter: 10 * time.Second}, nil
}

func (rum *RollingUpdateManager) performLeaderLastUpdate(ctx context.Context, cr *rqlitev1alpha1.RqliteCluster, currentSts, desiredSts *appsv1.StatefulSet) (ctrl.Result, error) {
	log := log.FromContext(ctx).WithValues("RqliteCluster", cr.Name, "Action", "LeaderLastUpdate")

	log.Info("Leader-last update policy - implementing standard rolling update for now")
	return rum.performStandardRollingUpdate(ctx, cr, currentSts, desiredSts)
}

func (rum *RollingUpdateManager) shouldRollback(ctx context.Context, cr *rqlitev1alpha1.RqliteCluster, sts *appsv1.StatefulSet) bool {
	updateStrategy := rum.getUpdateStrategy(cr)
	if !updateStrategy.RollbackOnFailure {
		return false
	}

	if cr.Status.UpdateStatus == nil || cr.Status.UpdateStatus.Phase != "InProgress" {
		return false
	}

	if cr.Status.UpdateStatus.StartTime != nil {
		timeout := time.Duration(*updateStrategy.UpdateTimeout) * time.Second
		if time.Since(cr.Status.UpdateStatus.StartTime.Time) > timeout {
			return true
		}
	}

	return rum.hasImagePullErrors(ctx, cr, sts)
}

func (rum *RollingUpdateManager) hasImagePullErrors(ctx context.Context, cr *rqlitev1alpha1.RqliteCluster, sts *appsv1.StatefulSet) bool {
	log := log.FromContext(ctx).WithValues("RqliteCluster", cr.Name, "Action", "CheckImagePullErrors")

	podList := &corev1.PodList{}
	selector := utils.LabelsForRqliteCluster(cr.Name)

	if err := rum.List(ctx, podList, client.InNamespace(cr.Namespace), client.MatchingLabels(selector)); err != nil {
		log.Error(err, "Failed to list pods for image pull error check")
		return false
	}

	for _, pod := range podList.Items {
		for _, containerStatus := range pod.Status.ContainerStatuses {
			if containerStatus.State.Waiting != nil {
				reason := containerStatus.State.Waiting.Reason
				if reason == "ImagePullBackOff" || reason == "ErrImagePull" {
					log.Info("Image pull error detected", "pod", pod.Name, "reason", reason)
					return true
				}
			}
		}

		for _, containerStatus := range pod.Status.InitContainerStatuses {
			if containerStatus.State.Waiting != nil {
				reason := containerStatus.State.Waiting.Reason
				if reason == "ImagePullBackOff" || reason == "ErrImagePull" {
					log.Info("Image pull error detected in init container", "pod", pod.Name, "reason", reason)
					return true
				}
			}
		}
	}

	return false
}

func (rum *RollingUpdateManager) performRollback(ctx context.Context, cr *rqlitev1alpha1.RqliteCluster, sts *appsv1.StatefulSet, previousVersion string) (ctrl.Result, error) {
	log := log.FromContext(ctx).WithValues("RqliteCluster", cr.Name, "Action", "Rollback")

	log.Info("Performing rollback", "from", cr.Status.UpdateStatus.TargetVersion, "to", previousVersion)

	previousImage := fmt.Sprintf("rqlite/rqlite:%s", previousVersion)
	patch := client.MergeFrom(sts.DeepCopy())
	sts.Spec.Template.Spec.Containers[0].Image = previousImage

	if err := rum.Patch(ctx, sts, patch); err != nil {
		log.Error(err, "Failed to patch StatefulSet for rollback")
		cr.Status.UpdateStatus.Phase = "Failed"
		cr.Status.UpdateStatus.LastError = fmt.Sprintf("Rollback failed: %v", err)
		return ctrl.Result{}, rum.Status().Update(ctx, cr)
	}

	cr.Status.UpdateStatus.Phase = "RolledBack"
	cr.Status.UpdateStatus.RollbackReason = "Image pull failure detected"
	cr.Status.UpdateStatus.CompletionTime = &metav1.Time{Time: time.Now()}
	cr.Status.UpdateStatus.CurrentVersion = previousVersion

	rollbackCondition := metav1.Condition{
		Type:               "RollbackCompleted",
		ObservedGeneration: cr.Generation,
		Status:             metav1.ConditionTrue,
		Reason:             "AutomaticRollback",
		Message:            fmt.Sprintf("Automatically rolled back from %s to %s due to update failure", cr.Status.UpdateStatus.TargetVersion, previousVersion),
		LastTransitionTime: metav1.Now(),
	}
	meta.SetStatusCondition(&cr.Status.Conditions, rollbackCondition)

	log.Info("Rollback completed successfully", "targetVersion", previousVersion)
	return ctrl.Result{RequeueAfter: 10 * time.Second}, rum.Status().Update(ctx, cr)
}

func (rum *RollingUpdateManager) UpdateVersionValidationFailedStatus(ctx context.Context, cr *rqlitev1alpha1.RqliteCluster, reason string) error {
	versionCondition := metav1.Condition{
		Type:               "VersionValid",
		ObservedGeneration: cr.Generation,
		Status:             metav1.ConditionFalse,
		Reason:             "ValidationFailed",
		Message:            reason,
		LastTransitionTime: metav1.Now(),
	}

	meta.SetStatusCondition(&cr.Status.Conditions, versionCondition)
	return rum.Status().Update(ctx, cr)
}
