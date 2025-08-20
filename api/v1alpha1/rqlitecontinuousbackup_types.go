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

// BackupInfo contains information about a stored database backup
type BackupInfo struct {
	// StartTime is when this backup was initiated
	StartTime metav1.Time `json:"startTime"`

	// EndTime is when this backup was completed
	EndTime metav1.Time `json:"endTime"`

	// Path is the storage path of this backup
	Path string `json:"path"`

	// Size is the size of the backup in bytes
	Size int64 `json:"size"`

	// EntryCount is the number of records at backup time (placeholder)
	EntryCount int64 `json:"entryCount"`

	// LastRaftIndex is the highest raft index at backup time
	LastRaftIndex uint64 `json:"lastRaftIndex"`
}

type WALSegmentInfo = BackupInfo

// RqliteContinuousBackupSpec defines the desired state of RqliteContinuousBackup
type RqliteContinuousBackupSpec struct {
	// ClusterName is the name of the RqliteCluster to backup
	//+kubebuilder:validation:Required
	ClusterName string `json:"clusterName"`

	// Interval defines how often to create database backups (e.g., "5m", "15m")
	//+kubebuilder:validation:Required
	//+kubebuilder:default="5m"
	Interval string `json:"interval"`

	// RetentionPeriod defines how long to keep database backups (e.g., "24h", "7d")
	//+kubebuilder:validation:Required
	//+kubebuilder:default="24h"
	RetentionPeriod string `json:"retentionPeriod"`

	// StoragePVC is the PVC name where database backups will be stored
	//+kubebuilder:validation:Required
	StoragePVC string `json:"storagePVC"`

	// Suspend indicates if the continuous backup should be suspended
	//+optional
	Suspend *bool `json:"suspend,omitempty"`
}

// RqliteContinuousBackupStatus defines the observed state of RqliteContinuousBackup
type RqliteContinuousBackupStatus struct {
	// Conditions represent the latest available observations of the backup's state
	//+optional
	Conditions []metav1.Condition `json:"conditions,omitempty"`

	// LastBackupTime is the end time of the last successfully created backup
	//+optional
	LastBackupTime *metav1.Time `json:"lastBackupTime,omitempty"`

	// +optional
	LastSegmentTime *metav1.Time `json:"lastSegmentTime,omitempty"`

	// LastRaftIndex is the last raft index that was processed
	//+optional
	LastRaftIndex uint64 `json:"lastRaftIndex,omitempty"`

	// BackupCount is the total number of database backups currently stored
	//+optional
	BackupCount int32 `json:"backupCount,omitempty"`

	// +optional
	SegmentCount int32 `json:"segmentCount,omitempty"`

	// TotalStorageUsed is the total storage used by database backups in bytes
	//+optional
	TotalStorageUsed int64 `json:"totalStorageUsed,omitempty"`

	// RecentBackups contains information about recent database backups
	//+optional
	RecentBackups []BackupInfo `json:"recentBackups,omitempty"`

	WALSegments []WALSegmentInfo `json:"walSegments,omitempty"`

	// NextScheduledRun is the next scheduled time for database backup
	//+optional
	NextScheduledRun *metav1.Time `json:"nextScheduledRun,omitempty"`

	// ObservedGeneration reflects the generation of the spec that was last processed
	//+optional
	ObservedGeneration int64 `json:"observedGeneration,omitempty"`

	// LastRetentionCleanup is the last time retention cleanup was performed
	//+optional
	LastRetentionCleanup *metav1.Time `json:"lastRetentionCleanup,omitempty"`

	// FilesDeletedLastCleanup is the number of files deleted in the last retention cleanup
	//+optional
	FilesDeletedLastCleanup int32 `json:"filesDeletedLastCleanup,omitempty"`

	// SpaceFreedLastCleanup is the amount of space freed in the last retention cleanup (bytes)
	//+optional
	SpaceFreedLastCleanup int64 `json:"spaceFreedLastCleanup,omitempty"`
}

//+kubebuilder:object:root=true
//+kubebuilder:subresource:status
//+kubebuilder:printcolumn:name="Cluster",type=string,JSONPath=".spec.clusterName"
//+kubebuilder:printcolumn:name="Interval",type=string,JSONPath=".spec.interval"
//+kubebuilder:printcolumn:name="Retention",type=string,JSONPath=".spec.retentionPeriod"
//+kubebuilder:printcolumn:name="Last Backup",type=date,JSONPath=".status.lastBackupTime"
//+kubebuilder:printcolumn:name="Age",type=date,JSONPath=".metadata.creationTimestamp"

// RqliteContinuousBackup is the Schema for the rqlitecontinuousbackups API
type RqliteContinuousBackup struct {
	metav1.TypeMeta   `json:",inline"`
	metav1.ObjectMeta `json:"metadata,omitempty"`

	Spec   RqliteContinuousBackupSpec   `json:"spec,omitempty"`
	Status RqliteContinuousBackupStatus `json:"status,omitempty"`
}

//+kubebuilder:object:root=true

// RqliteContinuousBackupList contains a list of RqliteContinuousBackup
type RqliteContinuousBackupList struct {
	metav1.TypeMeta `json:",inline"`
	metav1.ListMeta `json:"metadata,omitempty"`
	Items           []RqliteContinuousBackup `json:"items"`
}

func init() {
	SchemeBuilder.Register(&RqliteContinuousBackup{}, &RqliteContinuousBackupList{})
}
