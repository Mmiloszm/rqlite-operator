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

type RestorePhase string

const (
	RestorePhasePending   RestorePhase = "Pending"
	RestorePhaseLoading   RestorePhase = "Loading"
	RestorePhaseSucceeded RestorePhase = "Succeeded"
	RestorePhaseFailed    RestorePhase = "Failed"
)

// RqliteRestoreSpec defines the desired state of RqliteRestore
type RqliteRestoreSpec struct {
	//+kubebuilder:validation:Required
	ClusterName string `json:"clusterName"`

	// BackupName specifies a specific backup to restore from
	// Either BackupName or PointInTime must be specified, but not both
	//+optional
	BackupName *string `json:"backupName,omitempty"`

	// PointInTime specifies a timestamp to restore to using continuous backup
	// Either BackupName or PointInTime must be specified, but not both
	//+optional
	PointInTime *metav1.Time `json:"pointInTime,omitempty"`

	// ContinuousBackupName specifies which continuous backup to use for PITR
	// Required when PointInTime is specified
	//+optional
	ContinuousBackupName *string `json:"continuousBackupName,omitempty"`
}

// RqliteRestoreStatus defines the observed state of RqliteRestore
type RqliteRestoreStatus struct {
	//+optional
	Phase RestorePhase `json:"phase,omitempty"`

	//+optional
	Message string `json:"message,omitempty"`

	//+optional
	CompletionTime *metav1.Time `json:"completionTime,omitempty"`

	//+optional
	Conditions []metav1.Condition `json:"conditions,omitempty"`
}

//+kubebuilder:object:root=true
//+kubebuilder:subresource:status
//+kubebuilder:printcolumn:name="Cluster",type=string,JSONPath=".spec.clusterName"
//+kubebuilder:printcolumn:name="Backup",type=string,JSONPath=".spec.backupName"
//+kubebuilder:printcolumn:name="Status",type=string,JSONPath=".status.phase"
//+kubebuilder:printcolumn:name="Age",type=date,JSONPath=".metadata.creationTimestamp"

// RqliteRestore is the Schema for the rqliterestores API
type RqliteRestore struct {
	metav1.TypeMeta   `json:",inline"`
	metav1.ObjectMeta `json:"metadata,omitempty"`

	Spec   RqliteRestoreSpec   `json:"spec,omitempty"`
	Status RqliteRestoreStatus `json:"status,omitempty"`
}

//+kubebuilder:object:root=true

// RqliteRestoreList contains a list of RqliteRestore
type RqliteRestoreList struct {
	metav1.TypeMeta `json:",inline"`
	metav1.ListMeta `json:"metadata,omitempty"`
	Items           []RqliteRestore `json:"items"`
}

func init() {
	SchemeBuilder.Register(&RqliteRestore{}, &RqliteRestoreList{})
}
