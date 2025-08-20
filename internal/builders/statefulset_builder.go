package builders

import (
	"fmt"

	rqlitev1alpha1 "github.com/mmiloszm/rqlite-operator/api/v1alpha1"
	"github.com/mmiloszm/rqlite-operator/internal/utils"
	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	intstr "k8s.io/apimachinery/pkg/util/intstr"
	"sigs.k8s.io/controller-runtime/pkg/log"
)

func StatefulSetForRqliteCluster(cr *rqlitev1alpha1.RqliteCluster) (*appsv1.StatefulSet, error) {
	log := log.Log.WithValues("RqliteCluster", cr.Name, "Resource", "StatefulSetBuilder")

	ls := utils.LabelsForRqliteCluster(cr.Name)
	replicas := cr.Spec.Size
	version := cr.Spec.Version

	// Check if HPA is enabled if so don't set replicas to allow HPA to control scaling
	hpaEnabled := cr.Spec.AutoScaling != nil && cr.Spec.AutoScaling.Enabled &&
		cr.Spec.AutoScaling.HPA != nil && cr.Spec.AutoScaling.HPA.Enabled

	if hpaEnabled {
		log.Info("HPA is enabled, StatefulSet replicas will be managed by HPA")
	}

	if version == "" {
		version = "latest"
	}

	if err := utils.ValidateVersionFormat(version); err != nil {
		log.Error(err, "Invalid rqlite version specified", "version", version)
		return nil, fmt.Errorf("invalid rqlite version '%s': %w", version, err)
	}

	image := fmt.Sprintf("rqlite/rqlite:%s", version)

	log.Info("Using rqlite image", "image", image)

	storageCapacity, err := resource.ParseQuantity(cr.Spec.StorageCapacity)
	if err != nil {
		log.Error(err, "Failed to parse StorageCapacity", "Capacity", cr.Spec.StorageCapacity)
		return nil, fmt.Errorf("invalid StorageCapacity format '%s': %w", cr.Spec.StorageCapacity, err)
	}

	if cr.Spec.StorageClassName == "" {
		log.Info("StorageClassName not specified, will use default storage class")
	} else {
		log.Info("Using StorageClass", "name", cr.Spec.StorageClassName)
	}

	headlessServiceName := cr.Name + "-headless"
	containerArgs := []string{
		"-disco-mode=dns",
		fmt.Sprintf("-disco-config={\"name\":\"%s\"}", headlessServiceName),
		fmt.Sprintf("-bootstrap-expect=%d", replicas),
		"-join-interval=1s",
		"-join-attempts=120",
	}

	readinessProbe := &corev1.Probe{
		ProbeHandler: corev1.ProbeHandler{
			HTTPGet: &corev1.HTTPGetAction{
				Path:   "/readyz?noleader",
				Port:   intstr.FromString("http"),
				Scheme: corev1.URISchemeHTTP,
			},
		},
		InitialDelaySeconds: 2,
		TimeoutSeconds:      2,
		PeriodSeconds:       5,
		SuccessThreshold:    1,
		FailureThreshold:    3,
	}
	livenessProbe := &corev1.Probe{
		ProbeHandler: corev1.ProbeHandler{
			HTTPGet: &corev1.HTTPGetAction{
				Path:   "/readyz?noleader",
				Port:   intstr.FromString("http"),
				Scheme: corev1.URISchemeHTTP,
			},
		},
		InitialDelaySeconds: 5,
		TimeoutSeconds:      2,
		PeriodSeconds:       20,
		FailureThreshold:    3,
	}

	var terminationGracePeriodSeconds int64 = 10

	sts := &appsv1.StatefulSet{
		ObjectMeta: metav1.ObjectMeta{
			Name:      cr.Name,
			Namespace: cr.Namespace,
			Labels:    ls,
		},
		Spec: appsv1.StatefulSetSpec{
			Selector: &metav1.LabelSelector{
				MatchLabels: ls,
			},
			ServiceName: headlessServiceName,
			Template: corev1.PodTemplateSpec{
				ObjectMeta: metav1.ObjectMeta{
					Labels: ls,
				},
				Spec: corev1.PodSpec{
					TerminationGracePeriodSeconds: &terminationGracePeriodSeconds,
					Affinity:                      buildAffinityForRqliteCluster(cr),
					Containers: []corev1.Container{{
						Image:           image,
						Name:            "rqlite",
						ImagePullPolicy: corev1.PullIfNotPresent,
						Args:            containerArgs,
						Ports: []corev1.ContainerPort{
							{Name: "http", ContainerPort: 4001, Protocol: corev1.ProtocolTCP},
							{Name: "raft", ContainerPort: 4002, Protocol: corev1.ProtocolTCP},
						},
						VolumeMounts: []corev1.VolumeMount{
							{Name: "rqlite-data", MountPath: "/rqlite/file"},
						},
						ReadinessProbe: readinessProbe,
						LivenessProbe:  livenessProbe,
						Resources: corev1.ResourceRequirements{
							Requests: corev1.ResourceList{
								corev1.ResourceCPU:    resource.MustParse("100m"),
								corev1.ResourceMemory: resource.MustParse("128Mi"),
							},
							Limits: corev1.ResourceList{
								corev1.ResourceCPU:    resource.MustParse("500m"),
								corev1.ResourceMemory: resource.MustParse("512Mi"),
							},
						},
					}},
				},
			},
			VolumeClaimTemplates: []corev1.PersistentVolumeClaim{
				{
					ObjectMeta: metav1.ObjectMeta{
						Name:   "rqlite-data",
						Labels: ls,
					},
					Spec: corev1.PersistentVolumeClaimSpec{
						AccessModes: []corev1.PersistentVolumeAccessMode{corev1.ReadWriteOnce},
						Resources: corev1.VolumeResourceRequirements{
							Requests: corev1.ResourceList{
								corev1.ResourceStorage: storageCapacity,
							},
						},
						StorageClassName: func() *string {
							if cr.Spec.StorageClassName == "" {
								return nil // Use default storage class
							}
							return &cr.Spec.StorageClassName
						}(),
					},
				},
			},
			PodManagementPolicy: appsv1.ParallelPodManagement,
			UpdateStrategy:      buildUpdateStrategy(cr),
		},
	}

	if !hpaEnabled {
		sts.Spec.Replicas = &replicas
		log.Info("HPA disabled, setting StatefulSet replicas", "replicas", replicas)
	} else {
		log.Info("HPA enabled, StatefulSet replicas will be managed by HPA")
	}

	return sts, nil
}

func buildAffinityForRqliteCluster(cr *rqlitev1alpha1.RqliteCluster) *corev1.Affinity {
	if cr.Spec.PodAntiAffinity == nil || !cr.Spec.PodAntiAffinity.Enabled {
		return nil
	}

	topologyKey := cr.Spec.PodAntiAffinity.TopologyKey
	if topologyKey == "" {
		topologyKey = "kubernetes.io/hostname"
	}

	affinityType := cr.Spec.PodAntiAffinity.Type
	if affinityType == "" {
		affinityType = "soft"
	}

	labelSelector := &metav1.LabelSelector{
		MatchLabels: utils.LabelsForRqliteCluster(cr.Name),
	}

	podAffinityTerm := corev1.PodAffinityTerm{
		LabelSelector: labelSelector,
		TopologyKey:   topologyKey,
	}

	affinity := &corev1.Affinity{
		PodAntiAffinity: &corev1.PodAntiAffinity{},
	}

	if affinityType == "hard" {
		affinity.PodAntiAffinity.RequiredDuringSchedulingIgnoredDuringExecution = []corev1.PodAffinityTerm{
			podAffinityTerm,
		}
	} else {
		affinity.PodAntiAffinity.PreferredDuringSchedulingIgnoredDuringExecution = []corev1.WeightedPodAffinityTerm{
			{
				Weight:          100,
				PodAffinityTerm: podAffinityTerm,
			},
		}
	}

	return affinity
}

func buildUpdateStrategy(cr *rqlitev1alpha1.RqliteCluster) appsv1.StatefulSetUpdateStrategy {
	strategy := appsv1.StatefulSetUpdateStrategy{
		Type: appsv1.RollingUpdateStatefulSetStrategyType,
	}

	if cr.Spec.UpdateStrategy == nil {
		return strategy
	}

	if cr.Spec.UpdateStrategy.Type == "OnDelete" {
		strategy.Type = appsv1.OnDeleteStatefulSetStrategyType
		return strategy
	}

	if cr.Spec.UpdateStrategy.RollingUpdate != nil {
		rollingUpdate := &appsv1.RollingUpdateStatefulSetStrategy{}

		if cr.Spec.UpdateStrategy.RollingUpdate.MaxUnavailable != nil {
			maxUnavailable := intstr.FromInt32(*cr.Spec.UpdateStrategy.RollingUpdate.MaxUnavailable)
			rollingUpdate.MaxUnavailable = &maxUnavailable
		}

		strategy.RollingUpdate = rollingUpdate
	}

	return strategy
}
