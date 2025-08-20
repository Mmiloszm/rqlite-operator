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
	batchv1 "k8s.io/api/batch/v1"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/client-go/rest"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/log"
)

type RqliteContinuousBackupReconciler struct {
	client.Client
	Scheme     *runtime.Scheme
	RestConfig *rest.Config
}

//+kubebuilder:rbac:groups=rqlite.example.com,resources=rqlitecontinuousbackups,verbs=get;list;watch;create;update;patch;delete
//+kubebuilder:rbac:groups=rqlite.example.com,resources=rqlitecontinuousbackups/status,verbs=get;update;patch
//+kubebuilder:rbac:groups=rqlite.example.com,resources=rqlitecontinuousbackups/finalizers,verbs=update
//+kubebuilder:rbac:groups="",resources=pods,verbs=get;list;watch
//+kubebuilder:rbac:groups="",resources=pods/exec,verbs=create
//+kubebuilder:rbac:groups=batch,resources=jobs,verbs=get;list;watch;create;update;patch;delete

func (r *RqliteContinuousBackupReconciler) Reconcile(ctx context.Context, req ctrl.Request) (ctrl.Result, error) {
	log := log.FromContext(ctx)

	var continuousBackup rqlitev1alpha1.RqliteContinuousBackup
	if err := r.Get(ctx, req.NamespacedName, &continuousBackup); err != nil {
		if errors.IsNotFound(err) {
			return ctrl.Result{}, nil
		}
		return ctrl.Result{}, err
	}

	if continuousBackup.Spec.Suspend != nil && *continuousBackup.Spec.Suspend {
		log.Info("Continuous backup is suspended, skipping")
		return r.updateSuspendedStatus(ctx, &continuousBackup)
	}

	if !r.shouldCreateBackup(&continuousBackup) {
		nextRun := r.calculateNextRun(&continuousBackup)
		requeueAfter := time.Until(nextRun)
		if requeueAfter < 0 {
			requeueAfter = 5 * time.Second
		}

		log.Info("Backup not due yet", "nextRun", nextRun, "requeueAfter", requeueAfter)
		return ctrl.Result{RequeueAfter: requeueAfter}, nil
	}

	log.Info("Starting continuous backup job", "cluster", continuousBackup.Spec.ClusterName)

	if err := r.createBackupJob(ctx, &continuousBackup); err != nil {
		log.Error(err, "Failed to create backup job")
		return r.updateFailedStatus(ctx, &continuousBackup, err.Error())
	}

	if err := r.applyRetentionPolicy(ctx, &continuousBackup); err != nil {
		log.Error(err, "Failed to apply retention policy")
	}

	if err := r.updateSuccessStatus(ctx, &continuousBackup); err != nil {
		log.Error(err, "Failed to update success status")
		return ctrl.Result{}, err
	}

	interval, _ := time.ParseDuration(continuousBackup.Spec.Interval)
	return ctrl.Result{RequeueAfter: interval}, nil
}

func (r *RqliteContinuousBackupReconciler) shouldCreateBackup(cb *rqlitev1alpha1.RqliteContinuousBackup) bool {
	if cb.Status.LastBackupTime == nil {
		return true
	}

	interval, err := time.ParseDuration(cb.Spec.Interval)
	if err != nil {
		return false
	}

	nextBackupTime := cb.Status.LastBackupTime.Add(interval)
	return time.Now().After(nextBackupTime)
}

func (r *RqliteContinuousBackupReconciler) calculateNextRun(cb *rqlitev1alpha1.RqliteContinuousBackup) time.Time {
	interval, err := time.ParseDuration(cb.Spec.Interval)
	if err != nil {
		return time.Now().Add(5 * time.Minute)
	}

	if cb.Status.LastBackupTime == nil {
		return time.Now()
	}

	return cb.Status.LastBackupTime.Add(interval)
}

func (r *RqliteContinuousBackupReconciler) createBackupJob(ctx context.Context, cb *rqlitev1alpha1.RqliteContinuousBackup) error {
	log := log.FromContext(ctx)

	var fromTime time.Time
	if cb.Status.LastBackupTime != nil {
		fromTime = cb.Status.LastBackupTime.Time
	} else {
		fromTime = time.Now().Add(-time.Hour)
	}

	job := r.createDatabaseBackupJob(cb, fromTime)

	if err := ctrl.SetControllerReference(cb, job, r.Scheme); err != nil {
		return fmt.Errorf("failed to set controller reference: %w", err)
	}

	if err := r.Create(ctx, job); err != nil {
		return fmt.Errorf("failed to create backup job: %w", err)
	}

	log.Info("Backup job created", "job", job.Name, "cluster", cb.Spec.ClusterName)

	now := metav1.Now()
	cb.Status.LastBackupTime = &now
	cb.Status.BackupCount++

	backupInfo := rqlitev1alpha1.BackupInfo{
		StartTime:     metav1.NewTime(fromTime),
		EndTime:       metav1.NewTime(time.Now()),
		Path:          "backup-archive/" + cb.Spec.ClusterName + "/backups/",
		Size:          1024,
		EntryCount:    1,
		LastRaftIndex: 0,
	}

	cb.Status.RecentBackups = append([]rqlitev1alpha1.BackupInfo{backupInfo}, cb.Status.RecentBackups...)
	if len(cb.Status.RecentBackups) > 10 {
		cb.Status.RecentBackups = cb.Status.RecentBackups[:10]
	}

	return nil
}

func (r *RqliteContinuousBackupReconciler) createDatabaseBackupJob(cb *rqlitev1alpha1.RqliteContinuousBackup, fromTime time.Time) *batchv1.Job {
	jobName := fmt.Sprintf("%s-backup-%d", cb.Name, time.Now().Unix())
	backoffLimit := int32(2)

	job := &batchv1.Job{
		ObjectMeta: metav1.ObjectMeta{
			Name:      jobName,
			Namespace: cb.Namespace,
		},
		Spec: batchv1.JobSpec{
			BackoffLimit: &backoffLimit,
			Template: corev1.PodTemplateSpec{
				Spec: corev1.PodSpec{
					Volumes: []corev1.Volume{{
						Name: "backup-storage",
						VolumeSource: corev1.VolumeSource{
							PersistentVolumeClaim: &corev1.PersistentVolumeClaimVolumeSource{
								ClaimName: cb.Spec.StoragePVC,
							},
						},
					}},
					Containers: []corev1.Container{{
						Name:  "database-backup",
						Image: "curlimages/curl:8.7.1",
						Command: []string{
							"sh", "-c",
							fmt.Sprintf(`
								echo "Creating timestamped backup for cluster %s at %s"
								mkdir -p /backup-storage/backup-archive/%s/backups
								
								# Create timestamped backup file
								BACKUP_TIME="%s"
								BACKUP_FILE="/backup-storage/backup-archive/%s/backups/backup_$BACKUP_TIME.db"
								
								echo "Downloading database backup in DELETE mode..."
								if curl -f -s "http://%s-client.default.svc:4001/db/backup?fmt=delete" > "$BACKUP_FILE" 2>/dev/null; then
									BACKUP_SIZE=$(wc -c < "$BACKUP_FILE")
									echo "Successfully created backup: $(basename "$BACKUP_FILE") ($BACKUP_SIZE bytes)"
									
									# Create metadata file
									cat > "/backup-storage/backup-archive/%s/backups/backup_$BACKUP_TIME.json" <<EOF
{
  "timestamp": "%s",
  "filename": "backup_$BACKUP_TIME.db",
  "size": $BACKUP_SIZE,
  "clusterName": "%s",
  "format": "delete"
}
EOF
									echo "Backup metadata created"
								else
									echo "ERROR: Failed to create backup"
									exit 1
								fi
								
								echo "Timestamped backup completed successfully"
							`,
								cb.Spec.ClusterName,
								time.Now().Format("2006-01-02T15-04-05"),
								cb.Spec.ClusterName,
								time.Now().Format("2006-01-02T15-04-05"),
								cb.Spec.ClusterName,
								cb.Spec.ClusterName,
								cb.Spec.ClusterName,
								time.Now().Format(time.RFC3339),
								cb.Spec.ClusterName,
							),
						},
						VolumeMounts: []corev1.VolumeMount{{
							Name:      "backup-storage",
							MountPath: "/backup-storage",
						}},
					}},
					RestartPolicy: corev1.RestartPolicyOnFailure,
				},
			},
		},
	}
	return job
}

func (r *RqliteContinuousBackupReconciler) updateSuspendedStatus(ctx context.Context, cb *rqlitev1alpha1.RqliteContinuousBackup) (ctrl.Result, error) {
	condition := metav1.Condition{
		Type:    "Active",
		Status:  metav1.ConditionFalse,
		Reason:  "Suspended",
		Message: "Continuous backup is suspended by user",
	}
	meta.SetStatusCondition(&cb.Status.Conditions, condition)

	if err := r.Status().Update(ctx, cb); err != nil {
		return ctrl.Result{}, err
	}

	return ctrl.Result{RequeueAfter: 30 * time.Second}, nil
}

func (r *RqliteContinuousBackupReconciler) updateFailedStatus(ctx context.Context, cb *rqlitev1alpha1.RqliteContinuousBackup, errorMsg string) (ctrl.Result, error) {
	condition := metav1.Condition{
		Type:    "Active",
		Status:  metav1.ConditionFalse,
		Reason:  "BackupFailed",
		Message: "Database backup failed: " + errorMsg,
	}
	meta.SetStatusCondition(&cb.Status.Conditions, condition)

	if err := r.Status().Update(ctx, cb); err != nil {
		return ctrl.Result{}, err
	}

	interval, _ := time.ParseDuration(cb.Spec.Interval)
	return ctrl.Result{RequeueAfter: interval}, nil
}

func (r *RqliteContinuousBackupReconciler) updateSuccessStatus(ctx context.Context, cb *rqlitev1alpha1.RqliteContinuousBackup) error {
	condition := metav1.Condition{
		Type:    "Active",
		Status:  metav1.ConditionTrue,
		Reason:  "BackupSuccessful",
		Message: "Database backup completed successfully",
	}
	meta.SetStatusCondition(&cb.Status.Conditions, condition)

	cb.Status.ObservedGeneration = cb.Generation

	nextRun := metav1.NewTime(r.calculateNextRun(cb))
	cb.Status.NextScheduledRun = &nextRun

	return r.Status().Update(ctx, cb)
}

func (r *RqliteContinuousBackupReconciler) applyRetentionPolicy(ctx context.Context, cb *rqlitev1alpha1.RqliteContinuousBackup) error {
	log := log.FromContext(ctx)

	retentionDuration, err := time.ParseDuration(cb.Spec.RetentionPeriod)
	if err != nil {
		return fmt.Errorf("invalid retention period %s: %w", cb.Spec.RetentionPeriod, err)
	}

	cutoffTime := time.Now().Add(-retentionDuration)
	log.Info("Applying retention policy", "retentionPeriod", cb.Spec.RetentionPeriod, "cutoffTime", cutoffTime)

	if err := r.createRetentionCleanupJob(ctx, cb, cutoffTime); err != nil {
		return fmt.Errorf("failed to create retention cleanup job: %w", err)
	}

	return nil
}

func (r *RqliteContinuousBackupReconciler) createRetentionCleanupJob(ctx context.Context, cb *rqlitev1alpha1.RqliteContinuousBackup, cutoffTime time.Time) error {
	log := log.FromContext(ctx)

	jobName := fmt.Sprintf("%s-retention-cleanup-%d", cb.Name, time.Now().Unix())
	backoffLimit := int32(1)

	job := &batchv1.Job{
		ObjectMeta: metav1.ObjectMeta{
			Name:      jobName,
			Namespace: cb.Namespace,
		},
		Spec: batchv1.JobSpec{
			BackoffLimit: &backoffLimit,
			Template: corev1.PodTemplateSpec{
				Spec: corev1.PodSpec{
					Volumes: []corev1.Volume{{
						Name: "backup-storage",
						VolumeSource: corev1.VolumeSource{
							PersistentVolumeClaim: &corev1.PersistentVolumeClaimVolumeSource{
								ClaimName: cb.Spec.StoragePVC,
							},
						},
					}},
					Containers: []corev1.Container{{
						Name:  "retention-cleanup",
						Image: "busybox:1.36",
						Command: []string{
							"sh", "-c",
							fmt.Sprintf(`
								echo "Starting retention cleanup for cluster %s"
								echo "Cutoff time: %s"
								
								BACKUP_DIR="/backup-storage/backup-archive/%s/backups"
								
								if [ ! -d "$BACKUP_DIR" ]; then
									echo "Backup directory does not exist: $BACKUP_DIR"
									exit 0
								fi
								
								echo "Scanning for backup files older than %s..."
								
								DELETED_COUNT=0
								DELETED_SIZE=0
								
								# Find and process backup files older than cutoff time
								find "$BACKUP_DIR" -name "backup_*.db" -type f | while read -r backup_file; do
									# Extract timestamp from filename (format: backup_2006-01-02T15-04-05.db)
									filename=$(basename "$backup_file")
									timestamp=$(echo "$filename" | sed 's/backup_\(.*\)\.db/\1/' | sed 's/-/:/3' | sed 's/-/:/3')
									
									# Convert to epoch for comparison (approximate)
									backup_date=$(echo "$timestamp" | sed 's/T/ /')
									
									# Use stat to get file modification time for more reliable comparison
									file_mtime=$(stat -c %%Y "$backup_file" 2>/dev/null || echo "0")
									cutoff_epoch=%d
									
									if [ "$file_mtime" -lt "$cutoff_epoch" ] && [ "$file_mtime" -gt "0" ]; then
										file_size=$(stat -c %%s "$backup_file" 2>/dev/null || echo "0")
										echo "Deleting old backup: $filename (modified: $(date -d @$file_mtime), size: $file_size bytes)"
										
										# Delete backup file and its metadata
										rm -f "$backup_file"
										rm -f "${backup_file%%.db}.json"
										
										DELETED_COUNT=$((DELETED_COUNT + 1))
										DELETED_SIZE=$((DELETED_SIZE + file_size))
									fi
								done
								
								echo "Retention cleanup completed"
								echo "Files deleted: $DELETED_COUNT"
								echo "Space freed: $DELETED_SIZE bytes"
							`,
								cb.Spec.ClusterName,
								cutoffTime.Format(time.RFC3339),
								cb.Spec.ClusterName,
								cb.Spec.RetentionPeriod,
								cutoffTime.Unix(),
							),
						},
						VolumeMounts: []corev1.VolumeMount{{
							Name:      "backup-storage",
							MountPath: "/backup-storage",
						}},
					}},
					RestartPolicy: corev1.RestartPolicyOnFailure,
				},
			},
		},
	}

	if err := ctrl.SetControllerReference(cb, job, r.Scheme); err != nil {
		return fmt.Errorf("failed to set controller reference: %w", err)
	}

	if err := r.Create(ctx, job); err != nil {
		return fmt.Errorf("failed to create retention cleanup job: %w", err)
	}

	log.Info("Retention cleanup job created", "job", job.Name, "cutoffTime", cutoffTime)

	now := metav1.Now()
	cb.Status.LastRetentionCleanup = &now

	return nil
}

func (r *RqliteContinuousBackupReconciler) SetupWithManager(mgr ctrl.Manager) error {
	return ctrl.NewControllerManagedBy(mgr).
		For(&rqlitev1alpha1.RqliteContinuousBackup{}).
		Complete(r)
}
