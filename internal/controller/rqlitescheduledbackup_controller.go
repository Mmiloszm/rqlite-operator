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
	"sort"
	"time"

	"github.com/robfig/cron/v3"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/log"

	rqlitev1alpha1 "github.com/mmiloszm/rqlite-operator/api/v1alpha1"
)

type RqliteScheduledBackupReconciler struct {
	client.Client
	Scheme *runtime.Scheme
}

//+kubebuilder:rbac:groups=rqlite.example.com,resources=rqlitescheduledbackups,verbs=get;list;watch;create;update;patch;delete
//+kubebuilder:rbac:groups=rqlite.example.com,resources=rqlitescheduledbackups/status,verbs=get;update;patch
//+kubebuilder:rbac:groups=rqlite.example.com,resources=rqlitescheduledbackups/finalizers,verbs=update
//+kubebuilder:rbac:groups=rqlite.example.com,resources=rqlitebackups,verbs=get;list;watch;create;delete

func (r *RqliteScheduledBackupReconciler) Reconcile(ctx context.Context, req ctrl.Request) (ctrl.Result, error) {
	log := log.FromContext(ctx)

	var scheduledBackup rqlitev1alpha1.RqliteScheduledBackup
	if err := r.Get(ctx, req.NamespacedName, &scheduledBackup); err != nil {
		return ctrl.Result{}, client.IgnoreNotFound(err)
	}

	schedule, err := cron.ParseStandard(scheduledBackup.Spec.Schedule)
	if err != nil {
		log.Error(err, "Invalid cron schedule format")
		return ctrl.Result{}, nil
	}

	now := time.Now()
	lastRun := scheduledBackup.Status.LastScheduledTime
	if lastRun == nil {
		lastRun = &metav1.Time{Time: scheduledBackup.CreationTimestamp.Time}
	}
	nextRun := schedule.Next(lastRun.Time)
	log.Info("Cron schedule parsed", "now", now, "lastRun", lastRun, "nextRun", nextRun)

	if now.Before(nextRun) {
		sleepDuration := nextRun.Sub(now)
		log.Info("Not time to run yet, sleeping", "duration", sleepDuration)
		return ctrl.Result{RequeueAfter: sleepDuration}, nil
	}

	log.Info("Time to trigger a new backup")
	newBackup := &rqlitev1alpha1.RqliteBackup{
		ObjectMeta: metav1.ObjectMeta{
			Name:      fmt.Sprintf("%s-%d", scheduledBackup.Name, now.Unix()),
			Namespace: scheduledBackup.Namespace,
		},
		Spec: *scheduledBackup.Spec.BackupSpecTemplate.DeepCopy(),
	}
	newBackup.Spec.ClusterName = scheduledBackup.Spec.ClusterName

	if err := ctrl.SetControllerReference(&scheduledBackup, newBackup, r.Scheme); err != nil {
		log.Error(err, "Failed to set owner reference on new RqliteBackup")
		return ctrl.Result{}, err
	}

	if err := r.Create(ctx, newBackup); err != nil {
		log.Error(err, "Failed to create RqliteBackup resource")
		return ctrl.Result{}, err
	}
	log.Info("Successfully created new RqliteBackup", "backupName", newBackup.Name)

	scheduledBackup.Status.LastScheduledTime = &metav1.Time{Time: now}
	scheduledBackup.Status.LastBackupName = newBackup.Name
	if err := r.Status().Update(ctx, &scheduledBackup); err != nil {
		log.Error(err, "Failed to update RqliteScheduledBackup status")
		return ctrl.Result{}, err
	}

	if scheduledBackup.Spec.Retention != nil && scheduledBackup.Spec.Retention.KeepLast != nil {
		log.Info("Applying retention policy", "keepLast", *scheduledBackup.Spec.Retention.KeepLast)
		if err := r.applyRetentionPolicy(ctx, scheduledBackup); err != nil {
			log.Error(err, "Failed to apply retention policy")
		}
	}

	newNextRun := schedule.Next(now)
	sleepDuration := newNextRun.Sub(now)
	log.Info("Backup triggered, requeueing for the next schedule", "nextRun", newNextRun)
	return ctrl.Result{RequeueAfter: sleepDuration}, nil
}

func (r *RqliteScheduledBackupReconciler) applyRetentionPolicy(ctx context.Context, schedule rqlitev1alpha1.RqliteScheduledBackup) error {
	log := log.FromContext(ctx)

	var allBackups rqlitev1alpha1.RqliteBackupList
	if err := r.List(ctx, &allBackups, client.InNamespace(schedule.Namespace)); err != nil {
		return err
	}

	var ownedBackups []rqlitev1alpha1.RqliteBackup
	for _, backup := range allBackups.Items {
		if metav1.IsControlledBy(&backup, &schedule) && backup.Status.Phase == rqlitev1alpha1.BackupPhaseSucceeded {
			ownedBackups = append(ownedBackups, backup)
		}
	}

	sort.Slice(ownedBackups, func(i, j int) bool {
		return ownedBackups[i].CreationTimestamp.Time.After(ownedBackups[j].CreationTimestamp.Time)
	})

	keepCount := *schedule.Spec.Retention.KeepLast
	if len(ownedBackups) > keepCount {
		backupsToDelete := ownedBackups[keepCount:]
		log.Info("Retention policy: backups to delete", "count", len(backupsToDelete))
		for _, backupToDelete := range backupsToDelete {
			log.Info("Deleting old backup due to retention policy", "backupName", backupToDelete.Name)
			if err := r.Delete(ctx, &backupToDelete); err != nil {
				log.Error(err, "Failed to delete old backup", "backupName", backupToDelete.Name)
			}
		}
	}

	return nil
}

func (r *RqliteScheduledBackupReconciler) SetupWithManager(mgr ctrl.Manager) error {
	return ctrl.NewControllerManagedBy(mgr).
		For(&rqlitev1alpha1.RqliteScheduledBackup{}).
		Owns(&rqlitev1alpha1.RqliteBackup{}).
		Complete(r)
}
