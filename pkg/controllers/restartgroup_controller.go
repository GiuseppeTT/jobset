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
	"time"

	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/tools/record"
	"k8s.io/klog/v2"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/handler"
	"sigs.k8s.io/controller-runtime/pkg/reconcile"

	jobset "sigs.k8s.io/jobset/api/jobset/v1alpha2"
)

// RestartGroupReconciler reconciles a RestartGroup object
type RestartGroupReconciler struct {
	client.Client
	Scheme *runtime.Scheme
	Record record.EventRecorder
}

func NewRestartGroupReconciler(client client.Client, scheme *runtime.Scheme, record record.EventRecorder) *RestartGroupReconciler {
	return &RestartGroupReconciler{Client: client, Scheme: scheme, Record: record}
}

// SetupWithManager sets up the controller with the Manager.
func (r *RestartGroupReconciler) SetupWithManager(mgr ctrl.Manager) error {
	return ctrl.NewControllerManagedBy(mgr).
		For(&jobset.RestartGroup{}).
		Owns(&corev1.ConfigMap{}).
		Watches(
			&corev1.Pod{},
			handler.EnqueueRequestsFromMapFunc(func(ctx context.Context, obj client.Object) []reconcile.Request {
				labels := obj.GetLabels()
				restartGroupName, ok := labels["jobset.sigs.k8s.io/restart-group-name"] // TODO: Constant
				if !ok {
					return []reconcile.Request{}
				}
				restartGroupNamespace := obj.GetNamespace()
				return []reconcile.Request{
					{
						NamespacedName: types.NamespacedName{
							Name:      restartGroupName,
							Namespace: restartGroupNamespace,
						},
					},
				}
			}),
		).
		Complete(r)
}

// TODO: Use constants
func SetupRestartGroupIndexes(ctx context.Context, indexer client.FieldIndexer) error {
	if err := indexer.IndexField(ctx, &corev1.Pod{}, "podRestartGroupName", func(obj client.Object) []string {
		pod := obj.(*corev1.Pod)
		restartGroupName, ok := pod.Labels["jobset.sigs.k8s.io/restart-group-name"]
		if !ok {
			return nil
		}
		return []string{restartGroupName}
	}); err != nil {
		return err
	}

	if err := indexer.IndexField(ctx, &corev1.ConfigMap{}, ".metadata.controller", func(obj client.Object) []string {
		// grab the configMap object, extract the owner...
		configMap := obj.(*corev1.ConfigMap)
		owner := metav1.GetControllerOf(configMap)
		if owner == nil {
			return nil
		}
		// ...make sure it's a RestartGroup...
		if owner.APIVersion != apiGVStr || owner.Kind != "RestartGroup" {
			return nil
		}

		// ...and if so, return it
		return []string{owner.Name}
	}); err != nil {
		return err
	}

	return nil
}

// +kubebuilder:rbac:groups=jobset.x-k8s.io,resources=restartgroups,verbs=get;list;watch;create;update;patch;delete
// +kubebuilder:rbac:groups=jobset.x-k8s.io,resources=restartgroups/status,verbs=get;update;patch
// +kubebuilder:rbac:groups=jobset.x-k8s.io,resources=restartgroups/finalizers,verbs=update
// +kubebuilder:rbac:groups=core,resources=configmaps,verbs=get;list;watch;create;update;patch;delete

// Reconcile is part of the main kubernetes reconciliation loop which aims to
// move the current state of the cluster closer to the desired state.
func (r *RestartGroupReconciler) Reconcile(ctx context.Context, req ctrl.Request) (ctrl.Result, error) {
	// Get RestartGroup from apiserver
	var restartGroup jobset.RestartGroup
	if err := r.Get(ctx, req.NamespacedName, &restartGroup); err != nil {
		// we'll ignore not-found errors, since there is nothing we can do here.
		return ctrl.Result{}, client.IgnoreNotFound(err)
	}

	log := ctrl.LoggerFrom(ctx).WithValues("restartgroup", klog.KObj(&restartGroup))
	ctx = ctrl.LoggerInto(ctx, log)
	log.V(2).Info("Reconciling RestartGroup") // TODO: Set to V(4)

	// Get all pods managed by the RestartGroup
	// TODO: Use index to speed up listing
	// TODO: Turn into function
	var managedPods corev1.PodList
	if err := r.List(ctx, &managedPods, client.InNamespace(restartGroup.Namespace), client.MatchingFields{"podRestartGroupName": restartGroup.Name}); err != nil { // TODO: Use constants
		log.Error(err, "listing pods")
		return ctrl.Result{}, err
	}
	log.V(2).Info("Found pods", "count", len(managedPods.Items))

	// Get worker states
	// TODO: Add logging
	workerStates := getWorkerStates(restartGroup, managedPods)

	// Update restart state
	// TODO: Add logging
	if err := updateState(&restartGroup, workerStates); err != nil {
		log.Error(err, "updating restart group")
		return ctrl.Result{}, err
	}

	// Update RestartGroup status
	// TODO: Add logging
	if err := r.updateRestartGroupStatus(ctx, &restartGroup); apierrors.IsConflict(err) {
		return ctrl.Result{Requeue: true}, nil
	}

	// Broadcast if necessary
	if err := r.updateBroadcastConfigMap(ctx, &restartGroup); err != nil {
		log.Error(err, "broadcasting restart signal")
	}

	return ctrl.Result{}, nil
}

type WorkerState struct {
	StartedAt  *metav1.Time
	FinishedAt *metav1.Time
	ExitCode   *int32
}

func NewWorkerStateFromContainerState(containerState corev1.ContainerState) WorkerState {
	if containerState.Waiting != nil {
		return WorkerState{
			StartedAt:  nil,
			FinishedAt: nil,
			ExitCode:   nil,
		}
	} else if containerState.Running != nil {
		return WorkerState{
			StartedAt:  &containerState.Running.StartedAt,
			FinishedAt: nil,
			ExitCode:   nil,
		}
	} else if containerState.Terminated != nil {
		return WorkerState{
			StartedAt:  &containerState.Terminated.StartedAt,
			FinishedAt: &containerState.Terminated.FinishedAt,
			ExitCode:   &containerState.Terminated.ExitCode,
		}
	}
	return WorkerState{
		StartedAt:  nil,
		FinishedAt: nil,
		ExitCode:   nil,
	}
}

func getWorkerStates(restartGroup jobset.RestartGroup, managedPods corev1.PodList) map[string]WorkerState {
	workerStates := make(map[string]WorkerState)
	for _, pod := range managedPods.Items {
		workerId := pod.GenerateName
		for _, containerStatus := range pod.Status.ContainerStatuses {
			if containerStatus.Name == restartGroup.Spec.WorkerContainerName {
				workerState := NewWorkerStateFromContainerState(containerStatus.State)
				workerStates[workerId] = workerState
				break
			}
		}
	}
	return workerStates
}

func updateState(restartGroup *jobset.RestartGroup, workerStates map[string]WorkerState) error {
	if restartGroup.Status.RestartStartedAt == nil {
		restartGroup.Status.RestartStartedAt = &metav1.Time{}
	}
	if restartGroup.Status.RestartFinishedAt != nil { // Running
		return updateStateWhenRunning(restartGroup, workerStates)
	}
	return updateStateWhenRestarting(restartGroup, workerStates) // Restarting
}

func updateStateWhenRunning(restartGroup *jobset.RestartGroup, workerStates map[string]WorkerState) error {
	var minimumFinishedAt *metav1.Time
	for _, workerState := range workerStates {
		// Check if worker failed after last restart end
		if workerState.FinishedAt != nil && workerState.FinishedAt.After(restartGroup.Status.RestartFinishedAt.Time) && workerState.ExitCode != nil && *workerState.ExitCode != 0 {
			// Check if worker was the first to fail
			if minimumFinishedAt == nil || workerState.FinishedAt.Before(minimumFinishedAt) {
				minimumFinishedAt = workerState.FinishedAt
			}
		}
	}
	if minimumFinishedAt == nil {
		return nil
	}
	// Start a new group restart
	restartGroup.Status.RestartStartedAt = minimumFinishedAt
	restartGroup.Status.RestartFinishedAt = nil
	return nil
}

func updateStateWhenRestarting(restartGroup *jobset.RestartGroup, workerStates map[string]WorkerState) error {
	runningWorkerCount := int32(0)
	var maximumStartedAt *metav1.Time
	for _, workerState := range workerStates {
		// Check if worker started after restart start
		if workerState.StartedAt != nil && workerState.StartedAt.After(restartGroup.Status.RestartStartedAt.Time) {
			runningWorkerCount += 1
			// Check if worker was the last to start
			if maximumStartedAt == nil || workerState.StartedAt.After(maximumStartedAt.Time) {
				maximumStartedAt = workerState.StartedAt
			}
		}
	}
	if maximumStartedAt == nil {
		return nil
	}
	if runningWorkerCount > restartGroup.Spec.WorkerCount {
		return fmt.Errorf("number of running workers is bigger than total number of workers")
	}
	if runningWorkerCount < restartGroup.Spec.WorkerCount {
		return nil
	}
	// Finish current group restart
	restartGroup.Status.RestartFinishedAt = maximumStartedAt
	return nil
}

func (r *RestartGroupReconciler) updateRestartGroupStatus(ctx context.Context, restartGroup *jobset.RestartGroup) error {
	if err := r.Status().Update(ctx, restartGroup); err != nil {
		return err
	}
	return nil
}

// TODO: Add jobset-name label to it
// TODO: Add restart-group-name label to it
// TODO: Use constants
func (r *RestartGroupReconciler) updateBroadcastConfigMap(ctx context.Context, restartGroup *jobset.RestartGroup) error {
	log := ctrl.LoggerFrom(ctx)

	// No need to create / update configmap if restart start is nil / zero
	if restartGroup.Status.RestartStartedAt == nil || restartGroup.Status.RestartStartedAt.IsZero() {
		return nil
	}
	desiredData := map[string]string{
		"RestartStartedAt": restartGroup.Status.RestartStartedAt.Format(time.RFC3339Nano),
	}

	// Get configMap
	var childConfigMaps corev1.ConfigMapList
	if err := r.List(ctx, &childConfigMaps, client.InNamespace(restartGroup.Namespace), client.MatchingFields{".metadata.controller": restartGroup.Name}); err != nil {
		return err
	}
	if len(childConfigMaps.Items) > 1 {
		return fmt.Errorf("expected 0 or 1 ConfigMap, but found %d", len(childConfigMaps.Items))
	}

	// Create configMap if it doesn't exist
	if len(childConfigMaps.Items) == 0 {
		configMapName := restartGroup.Name + "-broadcast"
		newConfigMap := &corev1.ConfigMap{
			ObjectMeta: metav1.ObjectMeta{
				Name:      configMapName,
				Namespace: restartGroup.Namespace,
			},
			Data: desiredData,
		}
		if err := ctrl.SetControllerReference(restartGroup, newConfigMap, r.Scheme); err != nil {
			return fmt.Errorf("setting controller reference: %w", err)
		}
		log.V(2).Info("Creating broadcast configmap", "configmap", klog.KObj(newConfigMap))
		if err := r.Create(ctx, newConfigMap); err != nil {
			return fmt.Errorf("creating configmap: %w", err)
		}
	}

	childConfigMap := childConfigMaps.Items[0]

	// Skip update if nothing changed
	if currentRestartStartedAt, ok := childConfigMap.Data["RestartStartedAt"]; ok && currentRestartStartedAt == desiredData["RestartStartedAt"] {
		return nil
	}

	// Update configMap
	childConfigMap.Data = desiredData
	log.V(2).Info("Updating broadcast configmap", "configmap", klog.KObj(&childConfigMap))
	if err := r.Update(ctx, &childConfigMap); err != nil {
		return fmt.Errorf("updating configmap: %w", err)
	}
	return nil
}
