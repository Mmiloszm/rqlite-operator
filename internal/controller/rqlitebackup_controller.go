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
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/log"
)

type RqliteBackupReconciler struct {
	client.Client
	Scheme *runtime.Scheme
}

//+kubebuilder:rbac:groups=rqlite.example.com,resources=rqlitebackups,verbs=get;list;watch;create;update;patch;delete
//+kubebuilder:rbac:groups=rqlite.example.com,resources=rqlitebackups/status,verbs=get;update;patch
//+kubebuilder:rbac:groups=rqlite.example.com,resources=rqlitebackups/finalizers,verbs=update
//+kubebuilder:rbac:groups=batch,resources=jobs,verbs=get;list;watch;create;update;patch;delete
//+kubebuilder:rbac:groups=rqlite.example.com,resources=rqliteclusters,verbs=get;list;watch

func (r *RqliteBackupReconciler) Reconcile(ctx context.Context, req ctrl.Request) (ctrl.Result, error) {
	log := log.FromContext(ctx)

	var backup rqlitev1alpha1.RqliteBackup
	if err := r.Get(ctx, req.NamespacedName, &backup); err != nil {
		return ctrl.Result{}, client.IgnoreNotFound(err)
	}

	if backup.Status.Phase == rqlitev1alpha1.BackupPhaseSucceeded || backup.Status.Phase == rqlitev1alpha1.BackupPhaseFailed {
		return ctrl.Result{}, nil
	}

	backupJob := &batchv1.Job{}
	err := r.Get(ctx, types.NamespacedName{Name: backup.Name, Namespace: backup.Namespace}, backupJob)

	if err != nil {
		if apierrors.IsNotFound(err) {
			log.Info("Backup Job not found, creating a new one.")

			jobToCreate := builders.JobForRqliteBackup(&backup)
			if err := ctrl.SetControllerReference(&backup, jobToCreate, r.Scheme); err != nil {
				log.Error(err, "Failed to set owner reference on backup Job")
				return ctrl.Result{}, err
			}

			if err := r.Create(ctx, jobToCreate); err != nil {
				log.Error(err, "Failed to create backup Job")
				return ctrl.Result{}, err
			}

			backup.Status.Phase = rqlitev1alpha1.BackupPhaseRunning
			backup.Status.Message = "Backup job created and is running."
			if err := r.Status().Update(ctx, &backup); err != nil {
				log.Error(err, "Failed to update RqliteBackup status to Running")
				return ctrl.Result{}, err
			}
			return ctrl.Result{RequeueAfter: 15 * time.Second}, nil
		} else {
			log.Error(err, "Failed to get backup Job")
			return ctrl.Result{}, err
		}
	}

	if backupJob.Status.Succeeded > 0 {
		log.Info("Backup Job completed successfully")
		backup.Status.Phase = rqlitev1alpha1.BackupPhaseSucceeded
		backup.Status.Message = "Backup completed successfully."
		backup.Status.CompletionTime = backupJob.Status.CompletionTime

		filename := fmt.Sprintf("%s.sqlite", backup.Name)
		backup.Status.Path = fmt.Sprintf("pvc://%s/%s", backup.Spec.Storage.Pvc.Name, filename)
	} else if backupJob.Status.Failed > 0 {
		log.Info("Backup Job failed")
		backup.Status.Phase = rqlitev1alpha1.BackupPhaseFailed
		backup.Status.Message = "Backup job failed. Check Job logs for details."
	} else {
		log.Info("Backup Job is still running.")
		return ctrl.Result{RequeueAfter: 15 * time.Second}, nil
	}

	if err := r.Status().Update(ctx, &backup); err != nil {
		log.Error(err, "Failed to update final RqliteBackup status")
		return ctrl.Result{}, err
	}

	return ctrl.Result{}, nil
}

func (r *RqliteBackupReconciler) SetupWithManager(mgr ctrl.Manager) error {
	return ctrl.NewControllerManagedBy(mgr).
		For(&rqlitev1alpha1.RqliteBackup{}).
		Owns(&batchv1.Job{}).
		Complete(r)
}
