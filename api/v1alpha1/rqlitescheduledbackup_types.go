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

// RqliteScheduledBackupSpec defines the desired state of RqliteScheduledBackup
type RqliteScheduledBackupSpec struct {
	//+kubebuilder:validation:Required
	ClusterName string `json:"clusterName"`

	//+kubebuilder:validation:Required
	Schedule string `json:"schedule"`

	//+optional
	Retention *RetentionPolicy `json:"retention,omitempty"`

	//+kubebuilder:validation:Required
	BackupSpecTemplate RqliteBackupSpec `json:"backupSpecTemplate"`
}

// RetentionPolicy defines the policy for retaining old backups.
type RetentionPolicy struct {
	//+kubebuilder:validation:Minimum=1
	//+optional
	KeepLast *int `json:"keepLast,omitempty"`
}

// RqliteScheduledBackupStatus defines the observed state of RqliteScheduledBackup
type RqliteScheduledBackupStatus struct {
	//+optional
	LastScheduledTime *metav1.Time `json:"lastScheduledTime,omitempty"`

	//+optional
	LastBackupName string `json:"lastBackupName,omitempty"`

	//+optional
	Conditions []metav1.Condition `json:"conditions,omitempty"`
}

//+kubebuilder:object:root=true
//+kubebuilder:subresource:status
//+kubebuilder:printcolumn:name="Cluster",type=string,JSONPath=".spec.clusterName"
//+kubebuilder:printcolumn:name="Schedule",type=string,JSONPath=".spec.schedule"
//+kubebuilder:printcolumn:name="Last Backup",type=string,JSONPath=".status.lastBackupName"
//+kubebuilder:printcolumn:name="Last Run",type=date,JSONPath=".status.lastScheduledTime"

// RqliteScheduledBackup is the Schema for the rqlitescheduledbackups API
type RqliteScheduledBackup struct {
	metav1.TypeMeta   `json:",inline"`
	metav1.ObjectMeta `json:"metadata,omitempty"`

	Spec   RqliteScheduledBackupSpec   `json:"spec,omitempty"`
	Status RqliteScheduledBackupStatus `json:"status,omitempty"`
}

//+kubebuilder:object:root=true

// RqliteScheduledBackupList contains a list of RqliteScheduledBackup
type RqliteScheduledBackupList struct {
	metav1.TypeMeta `json:",inline"`
	metav1.ListMeta `json:"metadata,omitempty"`
	Items           []RqliteScheduledBackup `json:"items"`
}

func init() {
	SchemeBuilder.Register(&RqliteScheduledBackup{}, &RqliteScheduledBackupList{})
}
