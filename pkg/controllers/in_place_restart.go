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
	for _, rjob := range js.Spec.ReplicatedJobs {
		if _, ok := rjob.Template.Spec.Template.Labels[jobset.InPlaceRestartKey]; ok {
			return true
		}
	}

	return false
}

// TODO: Make JobSet fail if target generation exceeds max restarts
func (r *JobSetReconciler) updateGenerations(ctx context.Context, js *jobset.JobSet, updateStatusOpts *statusUpdateOpts) error {
	log := ctrl.LoggerFrom(ctx)

	desiredWorkerCount, err := countWorkers(js)
	if err != nil {
		log.Error(err, "counting workers")
		return err
	}

	grandchildPods, err := r.getGrandchildPods(ctx, js)
	if err != nil {
		log.Error(err, "getting grandchild pods")
		return err
	}

	generations, err := getGenerations(grandchildPods)
	if err != nil {
		log.Error(err, "getting generations")
		return err
	}

	// All workers are at the same generation
	// Make sure that the target generation matches this common value
	// to make sure the barriers are lift in case the agent sidecars are waiting
	if len(generations) == desiredWorkerCount && allEqual(generations) {
		updateTargetGenerationIfNecessary(js, generations, updateStatusOpts)
		return nil
	}

	// Otherwise, there is a mistmatch in the worker generations
	// In this case, make sure to outdate old generations
	updateOutdatedGenerationIfNecessary(js, generations, updateStatusOpts)

	return nil
}

func countWorkers(jobSet *jobset.JobSet) (int, error) {
	currentCount := 0
	for _, rjob := range jobSet.Spec.ReplicatedJobs {
		jobTemplate := rjob.Template
		podTemplate := jobTemplate.Spec.Template
		if _, ok := podTemplate.Labels[jobset.InPlaceRestartKey]; !ok {
			continue
		}
		if jobTemplate.Spec.Completions == nil || jobTemplate.Spec.Parallelism == nil || *jobTemplate.Spec.Completions != *jobTemplate.Spec.Parallelism {
			return 0, fmt.Errorf("in place restart requires jobTemplate.spec.completions == jobTemplate.spec.parallelism != nil for replicated job %s", rjob.Name)
		}
		currentCount += int(rjob.Replicas * *jobTemplate.Spec.Parallelism)
	}
	return currentCount, nil
}

func (r *JobSetReconciler) getGrandchildPods(ctx context.Context, js *jobset.JobSet) (*corev1.PodList, error) {
	var grandchildPods corev1.PodList
	if err := r.List(ctx, &grandchildPods, client.InNamespace(js.Namespace), client.MatchingFields{
		constants.PodJobSetKey: js.Namespace + "/" + js.Name,
	}); err != nil {
		return nil, err
	}
	return &grandchildPods, nil
}

func getGenerations(grandchildPods *corev1.PodList) ([]int, error) {
	generations := []int{}
	for _, pod := range grandchildPods.Items {
		rawGeneration, ok := pod.Annotations[jobset.InPlaceRestartGenerationKey]
		// Skip because there is a time between the Pod being created and the agent sidecar starting that the Pod does not container the generation annotation
		if !ok {
			continue
		}
		generation, err := strconv.Atoi(rawGeneration)
		if err != nil {
			return nil, fmt.Errorf("invalid value for annotation %s, must be integer", jobset.InPlaceRestartGenerationKey)
		}
		generations = append(generations, generation)
	}
	return generations, nil
}

func allEqual(values []int) bool {
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

func updateTargetGenerationIfNecessary(js *jobset.JobSet, generations []int, updateStatusOpts *statusUpdateOpts) {
	target := generations[0]
	if js.Status.InPlaceRestartTarget == int32(target) {
		return
	}
	js.Status.InPlaceRestartTarget = int32(target)
	updateStatusOpts.shouldUpdate = true
}

func updateOutdatedGenerationIfNecessary(js *jobset.JobSet, generations []int, updateStatusOpts *statusUpdateOpts) {
	if len(generations) == 0 {
		return
	}
	outdated := slices.Max(generations) - 1
	if js.Status.InPlaceRestartOutdated == int32(outdated) {
		return
	}
	js.Status.InPlaceRestartOutdated = int32(outdated)
	updateStatusOpts.shouldUpdate = true
}
