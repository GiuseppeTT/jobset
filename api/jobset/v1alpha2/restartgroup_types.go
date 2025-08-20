/*
Copyright 2023 The Kubernetes Authors.
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

package v1alpha2

import (
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

const (
	// TODO: Add description
	RestartGroupNameKey = "alpha.jobset.sigs.k8s.io/restart-group-name"
	// TODO: Add description
	WorkerContainerNameKey = "alpha.jobset.sigs.k8s.io/worker-container-name"
	// TODO: Add description
	BarrierStartedAtKey = "alpha.jobset.sigs.k8s.io/barrier-started-at"
	// TODO: Add description
	LiftBarrierStartedAtKey = "alpha.jobset.sigs.k8s.io/lift-barrier-started-at"
	// TODO: Add description
	RestartWorkerStartedAt = "alpha.jobset.sigs.k8s.io/restart-worker-started-at"
)

// RestartGroupSpec defines the desired state of RestartGroup
type RestartGroupSpec struct {
	// Container is the name of the worker container.
	// Worker containers are watched by the RestartGroup controller.
	// If any of the worker containers fails, a group restart is performed.
	// This only applies to Pods managed by the RestartGroup
	//+kubebuilder:validation:Required
	Container string `json:"container"`

	// Size is the number of worker containers in the managed Pods by the RestartGroup.
	//+kubebuilder:validation:Required
	Size int32 `json:"size"`
}

// RestartGroupStatus defines the observed state of RestartGroup
type RestartGroupStatus struct {
	// TODO: Description
	// +optional
	WorkerStatuses map[string]WorkerStatus `json:"workerStatuses,omitempty"`
}

// TODO: Description
type WorkerStatus struct {
	// TODO: Description
	BarrierStartedAt *metav1.Time `json:"barrierStartedAt"`
}

//+kubebuilder:object:root=true
//+kubebuilder:subresource:status

// RestartGroup is the Schema for the restartgroups API
type RestartGroup struct {
	metav1.TypeMeta   `json:",inline"`
	metav1.ObjectMeta `json:"metadata,omitempty"`

	Spec   RestartGroupSpec   `json:"spec,omitempty"`
	Status RestartGroupStatus `json:"status,omitempty"`
}

//+kubebuilder:object:root=true

// RestartGroupList contains a list of RestartGroup
type RestartGroupList struct {
	metav1.TypeMeta `json:",inline"`
	metav1.ListMeta `json:"metadata,omitempty"`
	Items           []RestartGroup `json:"items"`
}

func init() {
	SchemeBuilder.Register(&RestartGroup{}, &RestartGroupList{})
}
