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

type BackupPhase string

const (
	BackupPhasePending   BackupPhase = "Pending"
	BackupPhaseRunning   BackupPhase = "Running"
	BackupPhaseSucceeded BackupPhase = "Succeeded"
	BackupPhaseFailed    BackupPhase = "Failed"
)

type RqliteBackupSpec struct {
	//+kubebuilder:validation:Required
	ClusterName string `json:"clusterName"`

	//+kubebuilder:validation:Required
	Storage BackupStorage `json:"storage"`
}

// BackupStorage defines the storage configuration for a backup.
type BackupStorage struct {
	//+optional
	Pvc *PvcBackupStorage `json:"pvc,omitempty"`
}

// PvcBackupStorage defines the parameters for storing a backup on a PVC.
type PvcBackupStorage struct {
	//+kubebuilder:validation:Required
	Name string `json:"name"`

	//+optional
	Filename string `json:"filename,omitempty"`
}

// RqliteBackupStatus defines the observed state of RqliteBackup
type RqliteBackupStatus struct {
	//+optional
	Phase BackupPhase `json:"phase,omitempty"`

	//+optional
	Message string `json:"message,omitempty"`

	//+optional
	CompletionTime *metav1.Time `json:"completionTime,omitempty"`

	//+optional
	Path string `json:"path,omitempty"`

	//+optional
	Size int64 `json:"size,omitempty"`

	//+optional
	Conditions []metav1.Condition `json:"conditions,omitempty"`
}

//+kubebuilder:object:root=true
//+kubebuilder:subresource:status
//+kubebuilder:printcolumn:name="Cluster",type=string,JSONPath=".spec.clusterName"
//+kubebuilder:printcolumn:name="Status",type=string,JSONPath=".status.phase"
//+kubebuilder:printcolumn:name="Age",type=date,JSONPath=".metadata.creationTimestamp"

// RqliteBackup is the Schema for the rqlitebackups API
type RqliteBackup struct {
	metav1.TypeMeta   `json:",inline"`
	metav1.ObjectMeta `json:"metadata,omitempty"`

	Spec   RqliteBackupSpec   `json:"spec,omitempty"`
	Status RqliteBackupStatus `json:"status,omitempty"`
}

//+kubebuilder:object:root=true

// RqliteBackupList contains a list of RqliteBackup
type RqliteBackupList struct {
	metav1.TypeMeta `json:",inline"`
	metav1.ListMeta `json:"metadata,omitempty"`
	Items           []RqliteBackup `json:"items"`
}

func init() {
	SchemeBuilder.Register(&RqliteBackup{}, &RqliteBackupList{})
}
