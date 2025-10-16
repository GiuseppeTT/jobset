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
	"context"
	"fmt"
	"slices"
	"strconv"

	corev1 "k8s.io/api/core/v1"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	jobset "sigs.k8s.io/jobset/api/jobset/v1alpha2"
	"sigs.k8s.io/jobset/pkg/constants"
)

func isInPlaceRestartEnabled(js *jobset.JobSet) bool {
	return js.Spec.FailurePolicy.RestartStrategy == jobset.InPlaceRestart
}

// TODO: Make JobSet fail if epochs exceed maxRestarts
func (r *JobSetReconciler) reconcileEpochs(ctx context.Context, js *jobset.JobSet, updateStatusOpts *statusUpdateOpts) error {
	log := ctrl.LoggerFrom(ctx)

	childPods, err := r.getChildPods(ctx, js)
	if err != nil {
		log.Error(err, "getting child pods")
		return err
	}

	epochs, err := getEpochs(childPods)
	if err != nil {
		log.Error(err, "getting epochs")
		return err
	}

	expectedEpochsLength, err := getExpectedEpochsLength(js)
	if err != nil {
		log.Error(err, "getting expected epochs length")
		return err
	}

	// If all child Pods are at the same epoch, mark the current epoch as synced
	// This is done to make sure the barriers are lifted
	// This is indepontent
	if len(epochs) == expectedEpochsLength && allEqual(epochs) {
		reconcileMostRecentSyncedEpochIfNecessary(js, epochs, updateStatusOpts)
		return nil
	}

	// Otherwise, there is a mistmatch in the child Pod epochs, so deprecate all epochs but the latest one
	// This is done to make sure child Pods not in the latest epoch are restarted in place to reach the latest epoch
	// This is indepontent
	reconcileMostRecentDeprecatedEpochIfNecessary(js, epochs, updateStatusOpts)

	return nil
}

func (r *JobSetReconciler) getChildPods(ctx context.Context, js *jobset.JobSet) (*corev1.PodList, error) {
	var childPods corev1.PodList
	if err := r.List(ctx, &childPods, client.InNamespace(js.Namespace), client.MatchingFields{
		constants.PodJobSetKey: js.Namespace + "/" + js.Name,
	}); err != nil {
		return nil, err
	}
	return &childPods, nil
}

func getEpochs(childPods *corev1.PodList) ([]int32, error) {
	epochs := []int32{}
	for _, pod := range childPods.Items {
		rawEpoch, ok := pod.Annotations[jobset.EpochKey]
		// Skip because there is a time between the Pod being created and the agent sidecar starting that the Pod does not container the epoch annotation
		if !ok {
			continue
		}
		epoch, err := strconv.Atoi(rawEpoch)
		if err != nil {
			return nil, fmt.Errorf("invalid value for annotation %s, must be integer", jobset.EpochKey)
		}
		epochs = append(epochs, int32(epoch))
	}
	return epochs, nil
}

func getExpectedEpochsLength(js *jobset.JobSet) (int, error) {
	expectedLength := 0
	for _, rjob := range js.Spec.ReplicatedJobs {
		jobTemplate := rjob.Template
		if jobTemplate.Spec.Completions == nil || jobTemplate.Spec.Parallelism == nil || *jobTemplate.Spec.Completions != *jobTemplate.Spec.Parallelism {
			return 0, fmt.Errorf("in place restart requires jobTemplate.spec.completions == jobTemplate.spec.parallelism != nil for replicated job %s", rjob.Name)
		}
		expectedLength += int(rjob.Replicas * *jobTemplate.Spec.Parallelism)
	}
	return expectedLength, nil
}

func allEqual(values []int32) bool {
	if len(values) == 0 {
		return false
	}
	for _, value := range values {
		if value != values[0] {
			return false
		}
	}
	return true
}

func reconcileMostRecentSyncedEpochIfNecessary(js *jobset.JobSet, epochs []int32, updateStatusOpts *statusUpdateOpts) {
	syncedEpoch := epochs[0]
	if js.Status.SyncedEpoch == syncedEpoch {
		return
	}
	js.Status.SyncedEpoch = syncedEpoch
	updateStatusOpts.shouldUpdate = true
}

func reconcileMostRecentDeprecatedEpochIfNecessary(js *jobset.JobSet, epochs []int32, updateStatusOpts *statusUpdateOpts) {
	// Skip because slices.Max(epochs) can't be calculated with empty slices
	if len(epochs) == 0 {
		return
	}
	deprecatedEpoch := slices.Max(epochs) - 1
	if js.Status.DeprecatedEpoch == deprecatedEpoch {
		return
	}
	js.Status.DeprecatedEpoch = deprecatedEpoch
	updateStatusOpts.shouldUpdate = true
}
