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

package v1alpha1

import (
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

// EDIT THIS FILE!  THIS IS SCAFFOLDING FOR YOU TO OWN!
// NOTE: json tags are required.  Any new fields you add must have json tags for the fields to be serialized.

// PodAntiAffinitySpec defines pod anti-affinity configuration
type PodAntiAffinitySpec struct {
	// Enabled specifies whether pod anti-affinity should be configured
	// +kubebuilder:default=true
	Enabled bool `json:"enabled"`

	// TopologyKey specifies the topology key for anti-affinity
	// +kubebuilder:default="kubernetes.io/hostname"
	// +optional
	TopologyKey string `json:"topologyKey,omitempty"`

	// Type specifies the type of anti-affinity: "soft" for preferred or "hard" for required
	// +kubebuilder:validation:Enum=soft;hard
	// +kubebuilder:default="soft"
	// +optional
	Type string `json:"type,omitempty"`
}

// UpdateStrategySpec defines rolling update configuration
type UpdateStrategySpec struct {
	// Type specifies the update strategy type
	// +kubebuilder:validation:Enum=RollingUpdate;OnDelete
	// +kubebuilder:default="RollingUpdate"
	// +optional
	Type string `json:"type,omitempty"`

	// RollingUpdate configures rolling update parameters
	// +optional
	RollingUpdate *RollingUpdateSpec `json:"rollingUpdate,omitempty"`
}

// RollingUpdateSpec defines rolling update configuration
type RollingUpdateSpec struct {
	// MaxUnavailable specifies the max number of pods that can be unavailable during update
	// +kubebuilder:default=1
	// +optional
	MaxUnavailable *int32 `json:"maxUnavailable,omitempty"`

	// LeaderUpdatePolicy defines how leader updates are handled
	// +kubebuilder:validation:Enum=LeaderLast;LeaderFirst;Automatic
	// +kubebuilder:default="LeaderLast"
	// +optional
	LeaderUpdatePolicy string `json:"leaderUpdatePolicy,omitempty"`

	// UpdateTimeout specifies timeout for single pod update in seconds
	// +kubebuilder:default=300
	// +optional
	UpdateTimeout *int32 `json:"updateTimeout,omitempty"`

	// RollbackOnFailure enables automatic rollback on failed updates
	// +kubebuilder:default=true
	// +optional
	RollbackOnFailure bool `json:"rollbackOnFailure,omitempty"`
}

// AutoScalingSpec defines auto-scaling configuration
type AutoScalingSpec struct {
	// Enabled specifies whether auto-scaling should be enabled
	// +kubebuilder:default=false
	// +optional
	Enabled bool `json:"enabled,omitempty"`

	// HPA configures Horizontal Pod Autoscaler settings
	// +optional
	HPA *HPASpec `json:"hpa,omitempty"`

	// StorageExpansion configures automatic storage expansion
	// +optional
	StorageExpansion *StorageExpansionSpec `json:"storageExpansion,omitempty"`
}

// HPASpec defines Horizontal Pod Autoscaler configuration
type HPASpec struct {
	// Enabled specifies whether HPA should be enabled
	// +kubebuilder:default=false
	// +optional
	Enabled bool `json:"enabled,omitempty"`

	// MinReplicas specifies the minimum number of replicas
	// +kubebuilder:validation:Minimum=3
	// +kubebuilder:default=3
	// +optional
	MinReplicas *int32 `json:"minReplicas,omitempty"`

	// MaxReplicas specifies the maximum number of replicas
	// +kubebuilder:validation:Minimum=3
	// +kubebuilder:default=10
	// +optional
	MaxReplicas *int32 `json:"maxReplicas,omitempty"`

	// TargetCPUUtilizationPercentage specifies the target CPU utilization
	// +kubebuilder:validation:Minimum=1
	// +kubebuilder:validation:Maximum=100
	// +kubebuilder:default=70
	// +optional
	TargetCPUUtilizationPercentage *int32 `json:"targetCPUUtilizationPercentage,omitempty"`

	// TargetMemoryUtilizationPercentage specifies the target memory utilization
	// +kubebuilder:validation:Minimum=1
	// +kubebuilder:validation:Maximum=100
	// +kubebuilder:default=80
	// +optional
	TargetMemoryUtilizationPercentage *int32 `json:"targetMemoryUtilizationPercentage,omitempty"`

	// TargetRequestsPerSecond specifies the target requests per second per pod
	// +kubebuilder:validation:Minimum=1
	// +kubebuilder:default=50
	// +optional
	TargetRequestsPerSecond *int32 `json:"targetRequestsPerSecond,omitempty"`

	// CustomMetrics defines custom metrics for scaling decisions
	// +optional
	CustomMetrics []CustomMetricSpec `json:"customMetrics,omitempty"`

	// ScaleBehavior configures scaling behavior policies
	// +optional
	ScaleBehavior *ScaleBehaviorSpec `json:"scaleBehavior,omitempty"`
}

// CustomMetricSpec defines a custom metric for HPA scaling
type CustomMetricSpec struct {
	// Name is the name of the metric
	Name string `json:"name"`

	// Type specifies the metric type (Pods, Resource, External)
	// +kubebuilder:validation:Enum=Pods;Resource;External
	Type string `json:"type"`

	// TargetValue is the target value for the metric
	TargetValue string `json:"targetValue"`

	// Description provides a human-readable description of the metric
	// +optional
	Description string `json:"description,omitempty"`
}

// ScaleBehaviorSpec configures scaling behavior policies
type ScaleBehaviorSpec struct {
	// ScaleUp configures scale-up behavior
	// +optional
	ScaleUp *ScalingPolicySpec `json:"scaleUp,omitempty"`

	// ScaleDown configures scale-down behavior
	// +optional
	ScaleDown *ScalingPolicySpec `json:"scaleDown,omitempty"`
}

// ScalingPolicySpec defines scaling policy configuration
type ScalingPolicySpec struct {
	// StabilizationWindowSeconds specifies how long to wait before another scaling operation
	// +kubebuilder:default=300
	// +optional
	StabilizationWindowSeconds *int32 `json:"stabilizationWindowSeconds,omitempty"`

	// SelectPolicy specifies which policy to use (Min, Max, Disabled)
	// +kubebuilder:validation:Enum=Min;Max;Disabled
	// +kubebuilder:default="Max"
	// +optional
	SelectPolicy string `json:"selectPolicy,omitempty"`

	// Policies defines scaling rate policies
	// +optional
	Policies []ScalingPolicyRule `json:"policies,omitempty"`
}

// ScalingPolicyRule defines a single scaling policy rule
type ScalingPolicyRule struct {
	// Type specifies the scaling rule type (Pods, Percent)
	// +kubebuilder:validation:Enum=Pods;Percent
	Type string `json:"type"`

	// Value specifies the scaling amount
	Value int32 `json:"value"`

	// PeriodSeconds specifies the time period for this rule
	// +kubebuilder:default=60
	// +optional
	PeriodSeconds *int32 `json:"periodSeconds,omitempty"`
}

// StorageExpansionSpec defines automatic storage expansion configuration
type StorageExpansionSpec struct {
	// Enabled specifies whether storage auto-expansion should be enabled
	// +kubebuilder:default=false
	// +optional
	Enabled bool `json:"enabled,omitempty"`

	// ThresholdPercentage specifies the usage threshold to trigger expansion
	// +kubebuilder:validation:Minimum=1
	// +kubebuilder:validation:Maximum=99
	// +kubebuilder:default=80
	// +optional
	ThresholdPercentage *int32 `json:"thresholdPercentage,omitempty"`

	// ExpansionPercentage specifies how much to expand storage by
	// +kubebuilder:validation:Minimum=10
	// +kubebuilder:validation:Maximum=200
	// +kubebuilder:default=50
	// +optional
	ExpansionPercentage *int32 `json:"expansionPercentage,omitempty"`

	// MaxCapacity specifies the maximum storage capacity allowed
	// +optional
	MaxCapacity string `json:"maxCapacity,omitempty"`

	// CheckIntervalSeconds specifies how often to check storage usage
	// +kubebuilder:default=300
	// +optional
	CheckIntervalSeconds *int32 `json:"checkIntervalSeconds,omitempty"`
}

// NodeStatus represents the status of a single rqlite node
type NodeStatus struct {
	// ID is the unique identifier of the rqlite node
	ID string `json:"id"`

	// Address is the network address of the node
	Address string `json:"address"`

	// State represents the raft state of the node (Leader, Follower, Candidate)
	State string `json:"state"`

	// LastContact is the last time this node was contacted
	// +optional
	LastContact *metav1.Time `json:"lastContact,omitempty"`

	// Reachable indicates if the node is currently reachable
	Reachable bool `json:"reachable"`
}

// RqliteClusterSpec defines the desired state of RqliteCluster.
type RqliteClusterSpec struct {
	// INSERT ADDITIONAL SPEC FIELDS - desired state of cluster
	// Important: Run "make" to regenerate code after modifying this file

	// Size defines the number of nodes in the rqlite cluster.
	// +kubebuilder:validation:Minimum=1
	// +kubebuilder:default=3
	Size int32 `json:"size"`

	// Version defines the rqlite version to use.
	// +kubebuilder:validation:MinLength=1
	// +kubebuiler:default="latest"
	Version string `json:"version"`

	// StorageClassName specifies the StorageClass for PersistentVolumes
	// +optional
	StorageClassName string `json:"storageClassName,omitempty"`

	// StorageCapacity specifies the size of PersistentVolume for each replica
	// +optional
	// +kubebuilder:default="1Gi"
	StorageCapacity string `json:"storageCapacity,omitempty"`

	// PodAntiAffinity configures pod anti-affinity to spread pods across nodes
	// +optional
	PodAntiAffinity *PodAntiAffinitySpec `json:"podAntiAffinity,omitempty"`

	// UpdateStrategy configures rolling update behavior
	// +optional
	UpdateStrategy *UpdateStrategySpec `json:"updateStrategy,omitempty"`

	// AutoScaling configures automatic scaling behavior
	// +optional
	AutoScaling *AutoScalingSpec `json:"autoScaling,omitempty"`
}

// RqliteClusterStatus defines the observed state of RqliteCluster.
type RqliteClusterStatus struct {
	// INSERT ADDITIONAL STATUS FIELD - define observed state of cluster
	// Important: Run "make" to regenerate code after modifying this file

	// Conditions represent the latest available observations of an object's state.
	// +optional
	// +patchMergeKey=type
	// +patchStrategy=merge
	// +listType=map
	// +listMapKey=type
	Conditions []metav1.Condition `json:"conditions,omitempty" patchStrategy:"merge" patchMergeKey:"type"`

	// CurrentReplicas is the current number of rqlite pods manages by this cluster.
	// +optional
	CurrentReplicas int32 `json:"currentReplicas,omitempty"`

	// ReadyReplicas is the number of rqlite pods ready to serve requests.
	// +optional
	ReadyReplicas int32 `json:"readyReplicas,omitempty"`

	// Leader identifies the current leader node in the rqlite cluster
	// +optional
	Leader string `json:"leader,omitempty"`

	// LeaderID is the unique identifier of the current leader
	// +optional
	LeaderID string `json:"leaderID,omitempty"`

	// LastLeaderChange is the timestamp when leadership last changed
	// +optional
	LastLeaderChange *metav1.Time `json:"lastLeaderChange,omitempty"`

	// ClusterNodes contains information about all cluster nodes
	// +optional
	ClusterNodes []NodeStatus `json:"clusterNodes,omitempty"`

	// ObservedGeneration reflects the generation of the RqliteClusterSpec that was last processed by the controller.
	// +optional
	ObservedGeneration int64 `json:"observedGeneration,omitempty"`

	// UpdateStatus tracks the current rolling update status
	// +optional
	UpdateStatus *UpdateStatus `json:"updateStatus,omitempty"`

	// ScalingStatus tracks the current auto-scaling status
	// +optional
	ScalingStatus *ScalingStatus `json:"scalingStatus,omitempty"`
}

// UpdateStatus tracks rolling update progress
type UpdateStatus struct {
	// Phase indicates the current phase of the update
	// +kubebuilder:validation:Enum=Pending;InProgress;Paused;Completed;Failed;RolledBack
	// +optional
	Phase string `json:"phase,omitempty"`

	// CurrentVersion is the currently deployed version
	// +optional
	CurrentVersion string `json:"currentVersion,omitempty"`

	// TargetVersion is the target version for the update
	// +optional
	TargetVersion string `json:"targetVersion,omitempty"`

	// UpdatedReplicas is the number of replicas that have been updated
	// +optional
	UpdatedReplicas int32 `json:"updatedReplicas,omitempty"`

	// TotalReplicas is the total number of replicas in the cluster
	// +optional
	TotalReplicas int32 `json:"totalReplicas,omitempty"`

	// StartTime is when the update started
	// +optional
	StartTime *metav1.Time `json:"startTime,omitempty"`

	// CompletionTime is when the update completed
	// +optional
	CompletionTime *metav1.Time `json:"completionTime,omitempty"`

	// LastError contains the last error encountered during update
	// +optional
	LastError string `json:"lastError,omitempty"`

	// RollbackReason contains the reason for rollback if applicable
	// +optional
	RollbackReason string `json:"rollbackReason,omitempty"`
}

// ScalingStatus tracks auto-scaling progress and status
type ScalingStatus struct {
	// HPAEnabled indicates if HPA is currently active
	// +optional
	HPAEnabled bool `json:"hpaEnabled,omitempty"`

	// CurrentMetrics contains current metric values
	// +optional
	CurrentMetrics []MetricValue `json:"currentMetrics,omitempty"`

	// LastScaleTime is when the cluster was last scaled
	// +optional
	LastScaleTime *metav1.Time `json:"lastScaleTime,omitempty"`

	// LastScaleReason contains the reason for the last scaling operation
	// +optional
	LastScaleReason string `json:"lastScaleReason,omitempty"`

	// StorageExpansionEnabled indicates if storage auto-expansion is active
	// +optional
	StorageExpansionEnabled bool `json:"storageExpansionEnabled,omitempty"`

	// LastStorageExpansion is when storage was last expanded
	// +optional
	LastStorageExpansion *metav1.Time `json:"lastStorageExpansion,omitempty"`

	// StorageUsagePercentage contains current storage usage per pod
	// +optional
	StorageUsagePercentage map[string]int32 `json:"storageUsagePercentage,omitempty"`

	// Conditions represent scaling-related conditions
	// +optional
	// +patchMergeKey=type
	// +patchStrategy=merge
	// +listType=map
	// +listMapKey=type
	Conditions []metav1.Condition `json:"conditions,omitempty" patchStrategy:"merge" patchMergeKey:"type"`
}

// MetricValue represents a current metric value
type MetricValue struct {
	// Name is the name of the metric
	Name string `json:"name"`

	// Value is the current value of the metric
	Value string `json:"value"`

	// Timestamp is when this metric was last updated
	// +optional
	Timestamp *metav1.Time `json:"timestamp,omitempty"`
}

// +kubebuilder:object:root=true
// +kubebuilder:subresource:status

// RqliteCluster is the Schema for the rqliteclusters API.
type RqliteCluster struct {
	metav1.TypeMeta   `json:",inline"`
	metav1.ObjectMeta `json:"metadata,omitempty"`

	Spec   RqliteClusterSpec   `json:"spec,omitempty"`
	Status RqliteClusterStatus `json:"status,omitempty"`
}

// +kubebuilder:object:root=true

// RqliteClusterList contains a list of RqliteCluster.
type RqliteClusterList struct {
	metav1.TypeMeta `json:",inline"`
	metav1.ListMeta `json:"metadata,omitempty"`
	Items           []RqliteCluster `json:"items"`
}

func init() {
	SchemeBuilder.Register(&RqliteCluster{}, &RqliteClusterList{})
}
