package builders

import (
	"fmt"
	"strings"

	rqlitev1alpha1 "github.com/mmiloszm/rqlite-operator/api/v1alpha1"
	batchv1 "k8s.io/api/batch/v1"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

func JobForRqliteBackup(backup *rqlitev1alpha1.RqliteBackup) *batchv1.Job {
	fileName := fmt.Sprintf("%s.sqlite", backup.Name)
	rqliteServiceURL := fmt.Sprintf("http://%s-client.%s.svc:4001/db/backup", backup.Spec.ClusterName, backup.Namespace)
	backupPathInContainer := fmt.Sprintf("/backup-data/%s", fileName)
	backoffLimit := int32(4)

	job := &batchv1.Job{
		ObjectMeta: metav1.ObjectMeta{
			Name:      backup.Name,
			Namespace: backup.Namespace,
		},
		Spec: batchv1.JobSpec{
			BackoffLimit: &backoffLimit,
			Template: corev1.PodTemplateSpec{
				Spec: corev1.PodSpec{
					Volumes: []corev1.Volume{{
						Name: "backup-storage",
						VolumeSource: corev1.VolumeSource{
							PersistentVolumeClaim: &corev1.PersistentVolumeClaimVolumeSource{ClaimName: backup.Spec.Storage.Pvc.Name},
						},
					}},
					Containers: []corev1.Container{{
						Name:    "backup-agent",
						Image:   "curlimages/curl:8.7.1",
						Command: []string{"curl", "-L", "-f", "-o", backupPathInContainer, rqliteServiceURL},
						VolumeMounts: []corev1.VolumeMount{{
							Name: "backup-storage", MountPath: "/backup-data",
						}},
					}},
					RestartPolicy: corev1.RestartPolicyOnFailure,
				},
			},
		},
	}
	return job
}

func JobForRqliteRestore(restore *rqlitev1alpha1.RqliteRestore, backup *rqlitev1alpha1.RqliteBackup) *batchv1.Job {
	parts := strings.Split(strings.TrimPrefix(backup.Status.Path, "pvc://"), "/")
	pvcName := parts[0]
	fileName := parts[1]

	targetServiceURL := fmt.Sprintf("http://%s-client.%s.svc:4001/db/load", restore.Spec.ClusterName, restore.Namespace)

	backoffLimit := int32(2)
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
						Name: "backup-storage",
						VolumeSource: corev1.VolumeSource{
							PersistentVolumeClaim: &corev1.PersistentVolumeClaimVolumeSource{ClaimName: pvcName},
						},
					}},
					Containers: []corev1.Container{{
						Name:  "restore-agent",
						Image: "curlimages/curl:8.7.1",
						Command: []string{
							"curl", "-v", "-f", "-L", "-XPOST",
							targetServiceURL,
							"-H", "Content-type: application/octet-stream",
							"--data-binary",
							fmt.Sprintf("@/backup-data/%s", fileName),
						},
						VolumeMounts: []corev1.VolumeMount{{
							Name: "backup-storage", MountPath: "/backup-data",
						}},
					}},
					RestartPolicy: corev1.RestartPolicyOnFailure,
				},
			},
		},
	}
	return job
}
