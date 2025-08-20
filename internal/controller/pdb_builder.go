package controller

import (
	rqlitev1alpha1 "github.com/mmiloszm/rqlite-operator/api/v1alpha1"
	"github.com/mmiloszm/rqlite-operator/internal/utils"
	policyv1 "k8s.io/api/policy/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	intstr "k8s.io/apimachinery/pkg/util/intstr"
)

func podDisruptionBudgetForRqliteCluster(cr *rqlitev1alpha1.RqliteCluster) *policyv1.PodDisruptionBudget {
	labels := utils.LabelsForRqliteCluster(cr.Name)

	// For rqlite clusters, we want to maintain quorum during maintenance
	// For a 3-node cluster, allow at most 1 pod to be unavailable
	// For larger clusters, allow at most (size-1)/2 pods to be unavailable to maintain quorum
	var maxUnavailable intstr.IntOrString

	if cr.Spec.Size <= 3 {
		maxUnavailable = intstr.FromInt(1)
	} else {
		maxDisruptions := (cr.Spec.Size - 1) / 2
		if maxDisruptions < 1 {
			maxDisruptions = 1
		}
		maxUnavailable = intstr.FromInt(int(maxDisruptions))
	}

	pdb := &policyv1.PodDisruptionBudget{
		ObjectMeta: metav1.ObjectMeta{
			Name:      cr.Name + "-pdb",
			Namespace: cr.Namespace,
			Labels:    labels,
		},
		Spec: policyv1.PodDisruptionBudgetSpec{
			MaxUnavailable: &maxUnavailable,
			Selector: &metav1.LabelSelector{
				MatchLabels: map[string]string{
					"app.kubernetes.io/name":     "rqlite",
					"app.kubernetes.io/instance": cr.Name,
				},
			},
		},
	}

	return pdb
}
