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

	rqlitev1alpha1 "github.com/mmiloszm/rqlite-operator/api/v1alpha1"
	"github.com/mmiloszm/rqlite-operator/internal/builders"
	batchv1 "k8s.io/api/batch/v1"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/log"
)

type RqliteRestoreReconciler struct {
	client.Client
	Scheme *runtime.Scheme
}

//+kubebuilder:rbac:groups=rqlite.example.com,resources=rqliterestores,verbs=get;list;watch;create;update;patch;delete
//+kubebuilder:rbac:groups=rqlite.example.com,resources=rqliterestores/status,verbs=get;update;patch
//+kubebuilder:rbac:groups=rqlite.example.com,resources=rqliterestores/finalizers,verbs=update
//+kubebuilder:rbac:groups=batch,resources=jobs,verbs=get;list;watch;create;update;patch;delete
//+kubebuilder:rbac:groups=rqlite.example.com,resources=rqlitebackups,verbs=get;list;watch
//+kubebuilder:rbac:groups=rqlite.example.com,resources=rqliteclusters,verbs=get;list;watch

func (r *RqliteRestoreReconciler) Reconcile(ctx context.Context, req ctrl.Request) (ctrl.Result, error) {
	log := log.FromContext(ctx)

	var restore rqlitev1alpha1.RqliteRestore
	if err := r.Get(ctx, req.NamespacedName, &restore); err != nil {
		return ctrl.Result{}, client.IgnoreNotFound(err)
	}

	if restore.Status.Phase == rqlitev1alpha1.RestorePhaseSucceeded || restore.Status.Phase == rqlitev1alpha1.RestorePhaseFailed {
		return ctrl.Result{}, nil
	}

	if restore.Status.Phase == "" {
		restore.Status.Phase = rqlitev1alpha1.RestorePhasePending
		restore.Status.Message = "Validating restore prerequisites"
		if err := r.Status().Update(ctx, &restore); err != nil {
			log.Error(err, "Failed to initialize restore status")
			return ctrl.Result{}, err
		}
		return ctrl.Result{RequeueAfter: 5 * time.Second}, nil
	}

	switch restore.Status.Phase {
	case rqlitev1alpha1.RestorePhasePending:
		return r.handlePendingPhase(ctx, &restore)
	case rqlitev1alpha1.RestorePhaseLoading:
		return r.handleLoadingPhase(ctx, &restore)
	}

	return ctrl.Result{}, nil
}

func (r *RqliteRestoreReconciler) handlePendingPhase(ctx context.Context, restore *rqlitev1alpha1.RqliteRestore) (ctrl.Result, error) {
	if restore.Spec.PointInTime != nil {
		return r.handlePITRValidation(ctx, restore)
	} else if restore.Spec.BackupName != nil {
		return r.handleBackupValidation(ctx, restore)
	} else {
		restore.Status.Phase = rqlitev1alpha1.RestorePhaseFailed
		restore.Status.Message = "Either backupName or pointInTime must be specified"
		if err := r.Status().Update(ctx, restore); err != nil {
			return ctrl.Result{}, err
		}
		return ctrl.Result{}, nil
	}
}

func (r *RqliteRestoreReconciler) handleBackupValidation(ctx context.Context, restore *rqlitev1alpha1.RqliteRestore) (ctrl.Result, error) {
	log := log.FromContext(ctx)

	var backup rqlitev1alpha1.RqliteBackup
	if err := r.Get(ctx, types.NamespacedName{Name: *restore.Spec.BackupName, Namespace: restore.Namespace}, &backup); err != nil {
		if apierrors.IsNotFound(err) {
			restore.Status.Phase = rqlitev1alpha1.RestorePhaseFailed
			restore.Status.Message = "Referenced backup not found"
			if err := r.Status().Update(ctx, restore); err != nil {
				return ctrl.Result{}, err
			}
			return ctrl.Result{}, nil
		}
		return ctrl.Result{}, err
	}

	if backup.Status.Phase != rqlitev1alpha1.BackupPhaseSucceeded {
		restore.Status.Message = "Waiting for backup to complete successfully"
		if err := r.Status().Update(ctx, restore); err != nil {
			return ctrl.Result{}, err
		}
		return ctrl.Result{RequeueAfter: 30 * time.Second}, nil
	}

	if backup.Status.Path == "" {
		restore.Status.Phase = rqlitev1alpha1.RestorePhaseFailed
		restore.Status.Message = "Backup does not have a valid file path"
		if err := r.Status().Update(ctx, restore); err != nil {
			return ctrl.Result{}, err
		}
		return ctrl.Result{}, nil
	}

	var cluster rqlitev1alpha1.RqliteCluster
	if err := r.Get(ctx, types.NamespacedName{Name: restore.Spec.ClusterName, Namespace: restore.Namespace}, &cluster); err != nil {
		if apierrors.IsNotFound(err) {
			restore.Status.Phase = rqlitev1alpha1.RestorePhaseFailed
			restore.Status.Message = "Target cluster not found"
			if err := r.Status().Update(ctx, restore); err != nil {
				return ctrl.Result{}, err
			}
			return ctrl.Result{}, nil
		}
		return ctrl.Result{}, err
	}

	log.Info("Prerequisites validated, moving to loading phase")
	restore.Status.Phase = rqlitev1alpha1.RestorePhaseLoading
	restore.Status.Message = "Loading backup data into cluster using /db/load endpoint"
	if err := r.Status().Update(ctx, restore); err != nil {
		return ctrl.Result{}, err
	}
	return ctrl.Result{RequeueAfter: 5 * time.Second}, nil
}

func (r *RqliteRestoreReconciler) handlePITRValidation(ctx context.Context, restore *rqlitev1alpha1.RqliteRestore) (ctrl.Result, error) {
	log := log.FromContext(ctx)

	if restore.Spec.ContinuousBackupName == nil {
		restore.Status.Phase = rqlitev1alpha1.RestorePhaseFailed
		restore.Status.Message = "ContinuousBackupName is required for point-in-time restore"
		if err := r.Status().Update(ctx, restore); err != nil {
			return ctrl.Result{}, err
		}
		return ctrl.Result{}, nil
	}

	var continuousBackup rqlitev1alpha1.RqliteContinuousBackup
	if err := r.Get(ctx, types.NamespacedName{Name: *restore.Spec.ContinuousBackupName, Namespace: restore.Namespace}, &continuousBackup); err != nil {
		if apierrors.IsNotFound(err) {
			restore.Status.Phase = rqlitev1alpha1.RestorePhaseFailed
			restore.Status.Message = "Referenced continuous backup not found"
			if err := r.Status().Update(ctx, restore); err != nil {
				return ctrl.Result{}, err
			}
			return ctrl.Result{}, nil
		}
		return ctrl.Result{}, err
	}

	targetTime := restore.Spec.PointInTime.Time
	if continuousBackup.Status.LastSegmentTime != nil && targetTime.After(continuousBackup.Status.LastSegmentTime.Time) {
		restore.Status.Phase = rqlitev1alpha1.RestorePhaseFailed
		restore.Status.Message = fmt.Sprintf("Point-in-time %s is beyond available WAL data (latest: %s)",
			targetTime.Format(time.RFC3339), continuousBackup.Status.LastSegmentTime.Format(time.RFC3339))
		if err := r.Status().Update(ctx, restore); err != nil {
			return ctrl.Result{}, err
		}
		return ctrl.Result{}, nil
	}

	var cluster rqlitev1alpha1.RqliteCluster
	if err := r.Get(ctx, types.NamespacedName{Name: restore.Spec.ClusterName, Namespace: restore.Namespace}, &cluster); err != nil {
		if apierrors.IsNotFound(err) {
			restore.Status.Phase = rqlitev1alpha1.RestorePhaseFailed
			restore.Status.Message = "Target cluster not found"
			if err := r.Status().Update(ctx, restore); err != nil {
				return ctrl.Result{}, err
			}
			return ctrl.Result{}, nil
		}
		return ctrl.Result{}, err
	}

	log.Info("PITR validation passed, moving to loading phase")
	restore.Status.Phase = rqlitev1alpha1.RestorePhaseLoading
	restore.Status.Message = fmt.Sprintf("Loading data from point-in-time %s using continuous backup WAL segments",
		targetTime.Format(time.RFC3339))
	if err := r.Status().Update(ctx, restore); err != nil {
		return ctrl.Result{}, err
	}
	return ctrl.Result{RequeueAfter: 5 * time.Second}, nil
}

func (r *RqliteRestoreReconciler) handleLoadingPhase(ctx context.Context, restore *rqlitev1alpha1.RqliteRestore) (ctrl.Result, error) {
	if restore.Spec.PointInTime != nil {
		return r.handlePITRLoading(ctx, restore)
	} else {
		return r.handleBackupLoading(ctx, restore)
	}
}

func (r *RqliteRestoreReconciler) handleBackupLoading(ctx context.Context, restore *rqlitev1alpha1.RqliteRestore) (ctrl.Result, error) {
	log := log.FromContext(ctx)

	var backup rqlitev1alpha1.RqliteBackup
	if err := r.Get(ctx, types.NamespacedName{Name: *restore.Spec.BackupName, Namespace: restore.Namespace}, &backup); err != nil {
		log.Error(err, "Failed to get backup for restore")
		restore.Status.Phase = rqlitev1alpha1.RestorePhaseFailed
		restore.Status.Message = "Failed to retrieve backup information"
		if err := r.Status().Update(ctx, restore); err != nil {
			return ctrl.Result{}, err
		}
		return ctrl.Result{}, nil
	}

	restoreJob := &batchv1.Job{}
	err := r.Get(ctx, types.NamespacedName{Name: restore.Name, Namespace: restore.Namespace}, restoreJob)

	if err != nil {
		if apierrors.IsNotFound(err) {
			log.Info("Restore Job not found, creating a new one")

			jobToCreate := builders.JobForRqliteRestore(restore, &backup)
			if err := ctrl.SetControllerReference(restore, jobToCreate, r.Scheme); err != nil {
				log.Error(err, "Failed to set owner reference on restore Job")
				return ctrl.Result{}, err
			}

			if err := r.Create(ctx, jobToCreate); err != nil {
				log.Error(err, "Failed to create restore Job")
				return ctrl.Result{}, err
			}

			restore.Status.Message = "Restore job created, loading data via /db/load endpoint"
			if err := r.Status().Update(ctx, restore); err != nil {
				log.Error(err, "Failed to update restore status")
				return ctrl.Result{}, err
			}
			return ctrl.Result{RequeueAfter: 15 * time.Second}, nil
		} else {
			log.Error(err, "Failed to get restore Job")
			return ctrl.Result{}, err
		}
	}

	if restoreJob.Status.Succeeded > 0 {
		log.Info("Restore Job completed successfully")
		restore.Status.Phase = rqlitev1alpha1.RestorePhaseSucceeded
		restore.Status.Message = "Data successfully loaded from backup"
		restore.Status.CompletionTime = &metav1.Time{Time: time.Now()}

		condition := metav1.Condition{
			Type:               "Restored",
			Status:             metav1.ConditionTrue,
			Reason:             "RestoreCompleted",
			Message:            "Data has been successfully restored from backup using /db/load endpoint",
			LastTransitionTime: metav1.Now(),
		}
		restore.Status.Conditions = []metav1.Condition{condition}

	} else if restoreJob.Status.Failed > 0 {
		log.Info("Restore Job failed")
		restore.Status.Phase = rqlitev1alpha1.RestorePhaseFailed
		restore.Status.Message = "Restore job failed. Ensure cluster is ready and no concurrent writes are happening. Check Job logs for details."

		condition := metav1.Condition{
			Type:               "Restored",
			Status:             metav1.ConditionFalse,
			Reason:             "RestoreFailed",
			Message:            "Restore job failed during data loading",
			LastTransitionTime: metav1.Now(),
		}
		restore.Status.Conditions = []metav1.Condition{condition}

	} else {
		log.Info("Restore Job is still running")
		restore.Status.Message = "Data loading in progress via /db/load endpoint"
		if err := r.Status().Update(ctx, restore); err != nil {
			return ctrl.Result{}, err
		}
		return ctrl.Result{RequeueAfter: 15 * time.Second}, nil
	}

	if err := r.Status().Update(ctx, restore); err != nil {
		log.Error(err, "Failed to update final restore status")
		return ctrl.Result{}, err
	}

	return ctrl.Result{}, nil
}

func (r *RqliteRestoreReconciler) handlePITRLoading(ctx context.Context, restore *rqlitev1alpha1.RqliteRestore) (ctrl.Result, error) {
	log := log.FromContext(ctx)

	targetTime := restore.Spec.PointInTime.Time
	log.Info("Performing PITR restore", "targetTime", targetTime.Format(time.RFC3339))

	restoreJob := &batchv1.Job{}
	err := r.Get(ctx, types.NamespacedName{Name: restore.Name, Namespace: restore.Namespace}, restoreJob)

	if err != nil {
		if apierrors.IsNotFound(err) {
			log.Info("PITR Job not found, creating a new one")

			jobToCreate := r.createPITRJob(restore, targetTime)
			if err := ctrl.SetControllerReference(restore, jobToCreate, r.Scheme); err != nil {
				log.Error(err, "Failed to set owner reference on PITR Job")
				return ctrl.Result{}, err
			}

			if err := r.Create(ctx, jobToCreate); err != nil {
				log.Error(err, "Failed to create PITR Job")
				return ctrl.Result{}, err
			}

			restore.Status.Message = fmt.Sprintf("PITR job created, restoring to %s", targetTime.Format(time.RFC3339))
			if err := r.Status().Update(ctx, restore); err != nil {
				log.Error(err, "Failed to update PITR restore status")
				return ctrl.Result{}, err
			}
			return ctrl.Result{RequeueAfter: 15 * time.Second}, nil
		} else {
			log.Error(err, "Failed to get PITR Job")
			return ctrl.Result{}, err
		}
	}

	if restoreJob.Status.Succeeded > 0 {
		log.Info("PITR Job completed successfully")
		restore.Status.Phase = rqlitev1alpha1.RestorePhaseSucceeded
		restore.Status.Message = fmt.Sprintf("Successfully restored to point-in-time %s", targetTime.Format(time.RFC3339))
		restore.Status.CompletionTime = &metav1.Time{Time: time.Now()}

		condition := metav1.Condition{
			Type:               "Restored",
			Status:             metav1.ConditionTrue,
			Reason:             "PITRCompleted",
			Message:            fmt.Sprintf("Point-in-time restore to %s completed successfully", targetTime.Format(time.RFC3339)),
			LastTransitionTime: metav1.Now(),
		}
		restore.Status.Conditions = []metav1.Condition{condition}

	} else if restoreJob.Status.Failed > 0 {
		log.Info("PITR Job failed")
		restore.Status.Phase = rqlitev1alpha1.RestorePhaseFailed
		restore.Status.Message = fmt.Sprintf("PITR job failed for target time %s. Check Job logs for details.", targetTime.Format(time.RFC3339))

		condition := metav1.Condition{
			Type:               "Restored",
			Status:             metav1.ConditionFalse,
			Reason:             "PITRFailed",
			Message:            "Point-in-time restore job failed during execution",
			LastTransitionTime: metav1.Now(),
		}
		restore.Status.Conditions = []metav1.Condition{condition}

	} else {
		log.Info("PITR Job is still running")
		restore.Status.Message = fmt.Sprintf("PITR restore in progress for target time %s", targetTime.Format(time.RFC3339))
		if err := r.Status().Update(ctx, restore); err != nil {
			return ctrl.Result{}, err
		}
		return ctrl.Result{RequeueAfter: 15 * time.Second}, nil
	}

	if err := r.Status().Update(ctx, restore); err != nil {
		log.Error(err, "Failed to update final PITR restore status")
		return ctrl.Result{}, err
	}

	return ctrl.Result{}, nil
}

func (r *RqliteRestoreReconciler) createPITRJob(restore *rqlitev1alpha1.RqliteRestore, targetTime time.Time) *batchv1.Job {
	backoffLimit := int32(2)

	var continuousBackup rqlitev1alpha1.RqliteContinuousBackup
	pvcName := "wal-archive-storage"

	if restore.Spec.ContinuousBackupName != nil && *restore.Spec.ContinuousBackupName != "" {
		if err := r.Get(context.Background(), types.NamespacedName{
			Name:      *restore.Spec.ContinuousBackupName,
			Namespace: restore.Namespace,
		}, &continuousBackup); err == nil {
			pvcName = continuousBackup.Spec.StoragePVC
		}
	}

	job := &batchv1.Job{
		ObjectMeta: metav1.ObjectMeta{
			Name:      restore.Name,
			Namespace: restore.Namespace,
		},
		Spec: batchv1.JobSpec{
			BackoffLimit: &backoffLimit,
			Template: corev1.PodTemplateSpec{
				Spec: corev1.PodSpec{
					Volumes: []corev1.Volume{{
						Name: "wal-storage",
						VolumeSource: corev1.VolumeSource{
							PersistentVolumeClaim: &corev1.PersistentVolumeClaimVolumeSource{
								ClaimName: pvcName,
							},
						},
					}},
					Containers: []corev1.Container{{
						Name:  "pitr-restore-agent",
						Image: "curlimages/curl:8.7.1",
						Command: []string{
							"sh", "-c",
							fmt.Sprintf(`
								echo "Starting PITR restore to %s for cluster %s"
								
								# Check if timestamped backups exist
								BACKUPS_DIR="/wal-storage/backup-archive/%s/backups"
								if [ ! -d "$BACKUPS_DIR" ]; then
									echo "ERROR: No backups directory found at $BACKUPS_DIR"
									exit 1
								fi
								
								TARGET_TIME="%s"
								CLUSTER_URL="http://%s-client.default.svc:4001"
								echo "Searching for best backup before target time: $TARGET_TIME"
								
								# Find the most recent backup before target time
								SELECTED_BACKUP=""
								BEST_TIME=""
								
								for backup in "$BACKUPS_DIR"/backup_*.db; do
									if [ -f "$backup" ]; then
										# Extract timestamp from filename: backup_2025-08-13T21-36-47.db -> 2025-08-13T21:36:47
										BACKUP_TIMESTAMP=$(basename "$backup" | sed 's/backup_//; s/.db//' | sed 's/-\([0-9][0-9]\)-\([0-9][0-9]\)$/:\1:\2/')
										echo "Found backup: $(basename "$backup") at time: $BACKUP_TIMESTAMP"
										
										# Simple string comparison (works for ISO format)
										if [ "$BACKUP_TIMESTAMP" \< "$TARGET_TIME" ]; then
											if [ -z "$BEST_TIME" ] || [ "$BACKUP_TIMESTAMP" \> "$BEST_TIME" ]; then
												SELECTED_BACKUP="$backup"
												BEST_TIME="$BACKUP_TIMESTAMP"
											fi
										fi
									fi
								done
								
								if [ -z "$SELECTED_BACKUP" ]; then
									echo "ERROR: No suitable backup found before target time $TARGET_TIME"
									echo "Available backups:"
									ls -la "$BACKUPS_DIR"/backup_*.db 2>/dev/null || echo "No backups found"
									exit 1
								fi
								
								echo "Selected backup: $(basename "$SELECTED_BACKUP") (time: $BEST_TIME)"
								
								# Restore the selected backup
								echo "Loading backup into database..."
								if curl -v -f -X POST "$CLUSTER_URL/db/load" \
									-H "Content-Type: application/octet-stream" \
									--data-binary "@$SELECTED_BACKUP"; then
									echo "Successfully restored from backup: $(basename "$SELECTED_BACKUP")"
									echo "PITR restore to %s completed successfully"
									echo "Database restored to state at: $BEST_TIME"
								else
									echo "ERROR: Failed to load backup into database"
									exit 1
								fi
							`,
								targetTime.Format(time.RFC3339),
								restore.Spec.ClusterName,
								restore.Spec.ClusterName,
								targetTime.Format(time.RFC3339),
								restore.Spec.ClusterName,
								targetTime.Format(time.RFC3339),
							),
						},
						VolumeMounts: []corev1.VolumeMount{{
							Name:      "wal-storage",
							MountPath: "/wal-storage",
						}},
					}},
					RestartPolicy: corev1.RestartPolicyOnFailure,
				},
			},
		},
	}
	return job
}

func (r *RqliteRestoreReconciler) SetupWithManager(mgr ctrl.Manager) error {
	return ctrl.NewControllerManagedBy(mgr).
		For(&rqlitev1alpha1.RqliteRestore{}).
		Owns(&batchv1.Job{}).
		Complete(r)
}
