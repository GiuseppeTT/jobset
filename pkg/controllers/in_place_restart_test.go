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

package controllers

import (
	"testing"

	"github.com/go-logr/logr"

	"github.com/google/go-cmp/cmp"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/utils/ptr"

	jobset "sigs.k8s.io/jobset/api/jobset/v1alpha2"
	testutils "sigs.k8s.io/jobset/pkg/util/testing"
)

func TestIsInPlaceRestartStrategy(t *testing.T) {
	tests := []struct {
		name string
		js   *jobset.JobSet
		want bool
	}{
		{
			name: "in-place restart strategy",
			js: testutils.MakeJobSet("test-jobset", "default").
				FailurePolicy(&jobset.FailurePolicy{
					RestartStrategy: jobset.InPlaceRestart,
				}).Obj(),
			want: true,
		},
		{
			name: "recreate strategy",
			js: testutils.MakeJobSet("test-jobset", "default").
				FailurePolicy(&jobset.FailurePolicy{
					RestartStrategy: jobset.Recreate,
				}).Obj(),
			want: false,
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			if got := isInPlaceRestartStrategy(tc.js); got != tc.want {
				t.Errorf("isInPlaceRestartStrategy() = %v, want %v", got, tc.want)
			}
		})
	}
}


func TestGetMaxContainerRestartCount(t *testing.T) {
	tests := []struct {
		name      string
		childPods *corev1.PodList
		want      int32
	}{
		{
			name: "single pod single container",
			childPods: &corev1.PodList{
				Items: []corev1.Pod{
					{
						Status: corev1.PodStatus{
							ContainerStatuses: []corev1.ContainerStatus{
								{RestartCount: 5},
							},
						},
					},
				},
			},
			want: 5,
		},
		{
			name: "multiple pods multiple containers",
			childPods: &corev1.PodList{
				Items: []corev1.Pod{
					{
						Status: corev1.PodStatus{
							ContainerStatuses: []corev1.ContainerStatus{
								{RestartCount: 1},
								{RestartCount: 3},
							},
						},
					},
					{
						Status: corev1.PodStatus{
							ContainerStatuses: []corev1.ContainerStatus{
								{RestartCount: 2},
								{RestartCount: 4},
							},
						},
					},
				},
			},
			want: 4,
		},
		{
			name: "init containers",
			childPods: &corev1.PodList{
				Items: []corev1.Pod{
					{
						Status: corev1.PodStatus{
							InitContainerStatuses: []corev1.ContainerStatus{
								{RestartCount: 10},
							},
							ContainerStatuses: []corev1.ContainerStatus{
								{RestartCount: 5},
							},
						},
					},
				},
			},
			want: 10,
		},
		{
			name: "no pods",
			childPods: &corev1.PodList{
				Items: []corev1.Pod{},
			},
			want: 0,
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			if got := getMaxContainerRestartCount(tc.childPods); got != tc.want {
				t.Errorf("getMaxContainerRestartCount() = %v, want %v", got, tc.want)
			}
		})
	}
}


func TestGetInPlaceRestartAttempts(t *testing.T) {
	tests := []struct {
		name      string
		childPods *corev1.PodList
		want      []int32
		wantErr   bool
	}{
		{
			name: "pods with valid annotations",
			childPods: &corev1.PodList{
				Items: []corev1.Pod{
					testutils.MakePod("pod-1", "default").
						Annotations(map[string]string{"jobset.sigs.k8s.io/in-place-restart-attempt": "0"}).Obj(),
					testutils.MakePod("pod-2", "default").
						Annotations(map[string]string{"jobset.sigs.k8s.io/in-place-restart-attempt": "1"}).Obj(),
				},
			},
			want: []int32{0, 1},
		},
		{
			name: "pods with missing annotations",
			childPods: &corev1.PodList{
				Items: []corev1.Pod{
					testutils.MakePod("pod-1", "default").Obj(),
					testutils.MakePod("pod-2", "default").
						Annotations(map[string]string{"jobset.sigs.k8s.io/in-place-restart-attempt": "1"}).Obj(),
				},
			},
			want: []int32{1},
		},
		{
			name: "pods with invalid integer annotations",
			childPods: &corev1.PodList{
				Items: []corev1.Pod{
					testutils.MakePod("pod-1", "default").
						Annotations(map[string]string{"jobset.sigs.k8s.io/in-place-restart-attempt": "invalid"}).Obj(),
				},
			},
			wantErr: true,
		},
		{
			name: "pods with negative integer annotations",
			childPods: &corev1.PodList{
				Items: []corev1.Pod{
					testutils.MakePod("pod-1", "default").
						Annotations(map[string]string{"jobset.sigs.k8s.io/in-place-restart-attempt": "-1"}).Obj(),
				},
			},
			wantErr: true,
		},
		{
			name: "no pods",
			childPods: &corev1.PodList{
				Items: []corev1.Pod{},
			},
			want: []int32{},
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			got, err := getInPlaceRestartAttempts(tc.childPods)
			if (err != nil) != tc.wantErr {
				t.Errorf("getInPlaceRestartAttempts() error = %v, wantErr %v", err, tc.wantErr)
				return
			}
			if diff := cmp.Diff(tc.want, got); diff != "" {
				t.Errorf("getInPlaceRestartAttempts() mismatch (-want +got):\n%s", diff)
			}
		})
	}
}

func TestExceededMaxRestarts(t *testing.T) {
	tests := []struct {
		name                   string
		js                     *jobset.JobSet
		inPlaceRestartAttempts []int32
		want                   bool
	}{
		{
			name: "max restarts exceeded with 0 job recreations",
			js: testutils.MakeJobSet("test-jobset", "default").
				FailurePolicy(&jobset.FailurePolicy{
					MaxRestarts: 1,
				}).
				SetStatus(jobset.JobSetStatus{
					Restarts:                0,
					RestartsCountTowardsMax: 0,
				}).Obj(),
			inPlaceRestartAttempts: []int32{1, 2},
			want:                   true,
		},
		{
			name: "max restarts not exceeded with 0 job recreations",
			js: testutils.MakeJobSet("test-jobset", "default").
				FailurePolicy(&jobset.FailurePolicy{
					MaxRestarts: 2,
				}).
				SetStatus(jobset.JobSetStatus{
					Restarts:                0,
					RestartsCountTowardsMax: 0,
				}).Obj(),
			inPlaceRestartAttempts: []int32{1, 2},
			want:                   false,
		},
		{
			name: "max restarts exceeded with 1 job recreation",
			js: testutils.MakeJobSet("test-jobset", "default").
				FailurePolicy(&jobset.FailurePolicy{
					MaxRestarts: 1,
				}).
				SetStatus(jobset.JobSetStatus{
					Restarts:                1,
					RestartsCountTowardsMax: 1,
				}).Obj(),
			inPlaceRestartAttempts: []int32{1, 2},
			want:                   true,
		},
		{
			name: "max restarts not exceeded with 1 job recreation",
			js: testutils.MakeJobSet("test-jobset", "default").
				FailurePolicy(&jobset.FailurePolicy{
					MaxRestarts: 2,
				}).
				SetStatus(jobset.JobSetStatus{
					Restarts:                1,
					RestartsCountTowardsMax: 1,
				}).Obj(),
			inPlaceRestartAttempts: []int32{1, 2},
			want:                   false,
		},
		{
			name: "max restarts exceeded with uncounted restarts",
			js: testutils.MakeJobSet("test-jobset", "default").
				FailurePolicy(&jobset.FailurePolicy{
					MaxRestarts: 1,
				}).
				SetStatus(jobset.JobSetStatus{
					Restarts:                2,
					RestartsCountTowardsMax: 1,
				}).Obj(),
			inPlaceRestartAttempts: []int32{2, 3},
			// max(inPlaceRestartAttempts) - (restarts - restartsCountTowardsMax) > maxRestarts ?
			// 3 - (2 - 1) = 2 > 1 is true !
			want: true,
		},
		{
			name: "max restarts not exceeded with uncounted restarts",
			js: testutils.MakeJobSet("test-jobset", "default").
				FailurePolicy(&jobset.FailurePolicy{
					MaxRestarts: 1,
				}).
				SetStatus(jobset.JobSetStatus{
					Restarts:                2,
					RestartsCountTowardsMax: 1,
				}).Obj(),
			inPlaceRestartAttempts: []int32{1, 2},
			// max(inPlaceRestartAttempts) - (restarts - restartsCountTowardsMax) > maxRestarts ?
			// 2 - (2 - 1) = 1 > 1 is false !
			want: false,
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			if got := exceededMaxRestarts(tc.js, tc.inPlaceRestartAttempts); got != tc.want {
				t.Errorf("exceededMaxRestarts() = %v, want %v", got, tc.want)
			}
		})
	}
}

func TestGetExpectedInPlaceRestartAttemptsLength(t *testing.T) {
	tests := []struct {
		name    string
		js      *jobset.JobSet
		want    int
		wantErr bool
	}{
		{
			name: "valid replicated jobs",
			js: testutils.MakeJobSet("test-jobset", "default").
				ReplicatedJob(testutils.MakeReplicatedJob("rj-1").
					Replicas(2).
					Job(testutils.MakeJobTemplate("job-1", "default").
						Parallelism(2).
						Completions(2).Obj()).Obj()).
				ReplicatedJob(testutils.MakeReplicatedJob("rj-2").
					Replicas(1).
					Job(testutils.MakeJobTemplate("job-2", "default").
						Parallelism(3).
						Completions(3).Obj()).Obj()).
				Obj(),
			// 2*2 + 1*3 = 7
			want: 7,
		},
		{
			name: "invalid replicated job (completions != parallelism)",
			js: testutils.MakeJobSet("test-jobset", "default").
				ReplicatedJob(testutils.MakeReplicatedJob("rj-1").
					Replicas(1).
					Job(testutils.MakeJobTemplate("job-1", "default").
						Parallelism(2).
						Completions(1).Obj()).Obj()).
				Obj(),
			wantErr: true,
		},
		{
			name: "invalid replicated job (nil completions)",
			js: testutils.MakeJobSet("test-jobset", "default").
				ReplicatedJob(testutils.MakeReplicatedJob("rj-1").
					Replicas(1).
					Job(testutils.MakeJobTemplate("job-1", "default").
						Parallelism(2).Obj()).Obj()).
				Obj(),
			wantErr: true,
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			got, err := getExpectedInPlaceRestartAttemptsLength(tc.js)
			if (err != nil) != tc.wantErr {
				t.Errorf("getExpectedInPlaceRestartAttemptsLength() error = %v, wantErr %v", err, tc.wantErr)
				return
			}
			if got != tc.want {
				t.Errorf("getExpectedInPlaceRestartAttemptsLength() = %v, want %v", got, tc.want)
			}
		})
	}
}

func TestAllEqual(t *testing.T) {
	tests := []struct {
		name   string
		values []int32
		want   bool
	}{
		{
			name:   "empty slice",
			values: []int32{},
			want:   false,
		},
		{
			name:   "single value",
			values: []int32{1},
			want:   true,
		},
		{
			name:   "all equal values",
			values: []int32{1, 1, 1},
			want:   true,
		},
		{
			name:   "different values",
			values: []int32{1, 2, 1},
			want:   false,
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			if got := allEqual(tc.values); got != tc.want {
				t.Errorf("allEqual() = %v, want %v", got, tc.want)
			}
		})
	}
}

func TestUpdateCurrentInPlaceRestartAttempt(t *testing.T) {
	tests := []struct {
		name                   string
		js                     *jobset.JobSet
		inPlaceRestartAttempts []int32
		wantStatus             *int32
		wantUpdated            bool
	}{
		{
			name: "update status",
			js: testutils.MakeJobSet("test-jobset", "default").
				SetStatus(jobset.JobSetStatus{
					CurrentInPlaceRestartAttempt: ptr.To[int32](0),
				}).Obj(),
			inPlaceRestartAttempts: []int32{1, 1},
			wantStatus:             ptr.To[int32](1),
			wantUpdated:            true,
		},
		{
			name: "no update needed",
			js: testutils.MakeJobSet("test-jobset", "default").
				SetStatus(jobset.JobSetStatus{
					CurrentInPlaceRestartAttempt: ptr.To[int32](1),
				}).Obj(),
			inPlaceRestartAttempts: []int32{1, 1},
			wantStatus:             ptr.To[int32](1),
			wantUpdated:            false,
		},
		{
			name: "nil current attempt (all attempts 0) (JobSet synced for the first time since creation)",
			js: testutils.MakeJobSet("test-jobset", "default").
				SetStatus(jobset.JobSetStatus{
					CurrentInPlaceRestartAttempt: nil,
				}).Obj(),
			inPlaceRestartAttempts: []int32{0, 0},
			wantStatus:             ptr.To[int32](0),
			wantUpdated:            true,
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			opts := &statusUpdateOpts{}
			updateCurrentInPlaceRestartAttempt(logr.Discard(), tc.js, tc.inPlaceRestartAttempts, opts)
			if opts.shouldUpdate != tc.wantUpdated {
				t.Errorf("updateCurrentInPlaceRestartAttempt() updated = %v, want %v", opts.shouldUpdate, tc.wantUpdated)
			}
			if diff := cmp.Diff(tc.wantStatus, tc.js.Status.CurrentInPlaceRestartAttempt); diff != "" {
				t.Errorf("updateCurrentInPlaceRestartAttempt() status mismatch (-want +got):\n%s", diff)
			}
		})
	}
}

func TestUpdatePreviousInPlaceRestartAttempt(t *testing.T) {
	tests := []struct {
		name                   string
		js                     *jobset.JobSet
		inPlaceRestartAttempts []int32
		wantStatus             *int32
		wantUpdated            bool
	}{
		{
			name: "update status (max - 1 > current)",
			js: testutils.MakeJobSet("test-jobset", "default").
				SetStatus(jobset.JobSetStatus{
					PreviousInPlaceRestartAttempt: ptr.To[int32](0),
				}).Obj(),
			inPlaceRestartAttempts: []int32{2, 1},
			wantStatus:             ptr.To[int32](1),
			wantUpdated:            true,
		},
		{
			name: "no update needed (max - 1 == current)",
			js: testutils.MakeJobSet("test-jobset", "default").
				SetStatus(jobset.JobSetStatus{
					PreviousInPlaceRestartAttempt: ptr.To[int32](1),
				}).Obj(),
			inPlaceRestartAttempts: []int32{2, 1},
			wantStatus:             ptr.To[int32](1),
			wantUpdated:            false,
		},
		{
			name: "no update needed (max - 1 < current)",
			js: testutils.MakeJobSet("test-jobset", "default").
				SetStatus(jobset.JobSetStatus{
					PreviousInPlaceRestartAttempt: ptr.To[int32](1),
				}).Obj(),
			inPlaceRestartAttempts: []int32{1},
			wantStatus:             ptr.To[int32](1),
			wantUpdated:            false,
		},
		{
			name: "empty attempts",
			js: testutils.MakeJobSet("test-jobset", "default").
				SetStatus(jobset.JobSetStatus{
					PreviousInPlaceRestartAttempt: nil,
				}).Obj(),
			inPlaceRestartAttempts: []int32{},
			wantStatus:             nil,
			wantUpdated:            false,
		},
		{
			name: "empty previous attempt (first restart)",
			js: testutils.MakeJobSet("test-jobset", "default").
				SetStatus(jobset.JobSetStatus{
					PreviousInPlaceRestartAttempt: nil,
				}).Obj(),
			inPlaceRestartAttempts: []int32{0, 1},
			wantStatus:             ptr.To[int32](0),
			wantUpdated:            true,
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			opts := &statusUpdateOpts{}
			updatePreviousInPlaceRestartAttempt(logr.Discard(), tc.js, tc.inPlaceRestartAttempts, opts)
			if opts.shouldUpdate != tc.wantUpdated {
				t.Errorf("updatePreviousInPlaceRestartAttempt() updated = %v, want %v", opts.shouldUpdate, tc.wantUpdated)
			}
			if diff := cmp.Diff(tc.wantStatus, tc.js.Status.PreviousInPlaceRestartAttempt); diff != "" {
				t.Errorf("updatePreviousInPlaceRestartAttempt() status mismatch (-want +got):\n%s", diff)
			}
		})
	}
}
