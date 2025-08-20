package builders

import (
	rqlitev1alpha1 "github.com/mmiloszm/rqlite-operator/api/v1alpha1"
	"github.com/mmiloszm/rqlite-operator/internal/utils"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	intstr "k8s.io/apimachinery/pkg/util/intstr"
)

func HeadlessServiceForRqliteCluster(cr *rqlitev1alpha1.RqliteCluster) *corev1.Service {
	ls := utils.LabelsForRqliteCluster(cr.Name)
	serviceName := cr.Name + "-headless"

	svc := &corev1.Service{
		ObjectMeta: metav1.ObjectMeta{
			Name:      serviceName,
			Namespace: cr.Namespace,
			Labels:    ls,
		},
		Spec: corev1.ServiceSpec{
			ClusterIP: corev1.ClusterIPNone,
			Ports: []corev1.ServicePort{
				{Name: "http", Port: 4001, Protocol: corev1.ProtocolTCP, TargetPort: intstr.FromString("http")},
				{Name: "raft", Port: 4002, Protocol: corev1.ProtocolTCP, TargetPort: intstr.FromString("raft")},
			},
			Selector:                 ls,
			PublishNotReadyAddresses: true,
		},
	}

	return svc
}

func ClientServiceForRqliteCluster(cr *rqlitev1alpha1.RqliteCluster) *corev1.Service {
	ls := utils.LabelsForRqliteCluster(cr.Name)
	serviceName := cr.Name + "-client"

	svc := &corev1.Service{
		ObjectMeta: metav1.ObjectMeta{
			Name:      serviceName,
			Namespace: cr.Namespace,
			Labels:    ls,
		},
		Spec: corev1.ServiceSpec{
			Ports: []corev1.ServicePort{
				{Name: "http", Port: 4001, Protocol: corev1.ProtocolTCP, TargetPort: intstr.FromString("http")},
			},
			Selector: ls,
		},
	}

	return svc
}
