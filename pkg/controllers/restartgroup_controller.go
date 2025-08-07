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

const (
	RestartGroupKind                 = "RestartGroup"
	PodRestartGroupNameKey           = "podRestartGroupName"
	ConfigMapOwnerKey                = ".metadata.controller"
	RestartGroupCreationFailedReason = "RestartGroupCreationFailed"
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
				restartGroupName, ok := labels[jobset.RestartGroupNameKey]
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

func SetupRestartGroupIndexes(ctx context.Context, indexer client.FieldIndexer) error {
	if err := indexer.IndexField(ctx, &corev1.Pod{}, PodRestartGroupNameKey, func(obj client.Object) []string {
		pod := obj.(*corev1.Pod)
		restartGroupName, ok := pod.Labels[jobset.RestartGroupNameKey]
		if !ok {
			return nil
		}
		return []string{restartGroupName}
	}); err != nil {
		return err
	}

	if err := indexer.IndexField(ctx, &corev1.ConfigMap{}, ConfigMapOwnerKey, func(obj client.Object) []string {
		configMap := obj.(*corev1.ConfigMap)
		owner := metav1.GetControllerOf(configMap)
		if owner == nil {
			return nil
		}
		if owner.APIVersion != apiGVStr || owner.Kind != RestartGroupKind {
			return nil
		}
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

// TODO: Add events
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
	log.V(4).Info("Reconciling RestartGroup")

	// Get all pods managed by the RestartGroup
	// TODO: Turn into function
	var managedPods corev1.PodList
	if err := r.List(ctx, &managedPods, client.InNamespace(restartGroup.Namespace), client.MatchingFields{PodRestartGroupNameKey: restartGroup.Name}); err != nil {
		log.Error(err, "listing pods")
		return ctrl.Result{}, err
	}
	log.V(2).Info("Found pods", "count", len(managedPods.Items))

	// Get container states
	containerStates := getContainerStates(restartGroup, managedPods)

	// Update restart state
	if err := updateState(&restartGroup, containerStates); err != nil {
		log.Error(err, "updating restart group")
		return ctrl.Result{}, err
	}

	// Update RestartGroup status
	if err := r.updateRestartGroupStatus(ctx, &restartGroup); apierrors.IsConflict(err) {
		return ctrl.Result{Requeue: true}, nil
	}

	// Broadcast if necessary
	if err := r.updateBroadcastConfigMap(ctx, &restartGroup); err != nil {
		log.Error(err, "broadcasting restart signal")
	}

	return ctrl.Result{}, nil
}

type ContainerState struct {
	StartedAt  *metav1.Time
	FinishedAt *metav1.Time
	ExitCode   *int32
}

func NewContainerState(containerState corev1.ContainerState) ContainerState {
	if containerState.Waiting != nil {
		return ContainerState{
			StartedAt:  nil,
			FinishedAt: nil,
			ExitCode:   nil,
		}
	} else if containerState.Running != nil {
		return ContainerState{
			StartedAt:  &containerState.Running.StartedAt,
			FinishedAt: nil,
			ExitCode:   nil,
		}
	} else if containerState.Terminated != nil {
		return ContainerState{
			StartedAt:  &containerState.Terminated.StartedAt,
			FinishedAt: &containerState.Terminated.FinishedAt,
			ExitCode:   &containerState.Terminated.ExitCode,
		}
	}
	return ContainerState{
		StartedAt:  nil,
		FinishedAt: nil,
		ExitCode:   nil,
	}
}

func getContainerStates(restartGroup jobset.RestartGroup, managedPods corev1.PodList) map[string]ContainerState {
	containerStates := make(map[string]ContainerState)
	for _, pod := range managedPods.Items {
		id := pod.GenerateName
		for _, containerStatus := range pod.Status.ContainerStatuses {
			if containerStatus.Name == restartGroup.Spec.Container {
				containerState := NewContainerState(containerStatus.State)
				containerStates[id] = containerState
				break
			}
		}
	}
	return containerStates
}

func updateState(restartGroup *jobset.RestartGroup, containerStates map[string]ContainerState) error {
	if restartGroup.Status.RestartStartedAt == nil {
		restartGroup.Status.RestartStartedAt = &metav1.Time{}
	}
	if restartGroup.Status.RestartFinishedAt != nil { // Running
		return updateStateWhenRunning(restartGroup, containerStates)
	}
	return updateStateWhenRestarting(restartGroup, containerStates) // Restarting
}

func updateStateWhenRunning(restartGroup *jobset.RestartGroup, containerStates map[string]ContainerState) error {
	var minimumFinishedAt *metav1.Time
	for _, containerState := range containerStates {
		// Check if container failed after last restart end
		if containerState.FinishedAt != nil && containerState.FinishedAt.After(restartGroup.Status.RestartFinishedAt.Time) && containerState.ExitCode != nil && *containerState.ExitCode != 0 {
			// Check if container was the first to fail
			if minimumFinishedAt == nil || containerState.FinishedAt.Before(minimumFinishedAt) {
				minimumFinishedAt = containerState.FinishedAt
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

func updateStateWhenRestarting(restartGroup *jobset.RestartGroup, containerStates map[string]ContainerState) error {
	runningContainerCount := int32(0)
	var maximumStartedAt *metav1.Time
	for _, containerState := range containerStates {
		// Check if container started after restart start
		if containerState.StartedAt != nil && containerState.StartedAt.After(restartGroup.Status.RestartStartedAt.Time) {
			runningContainerCount += 1
			// Check if container was the last to start
			if maximumStartedAt == nil || containerState.StartedAt.After(maximumStartedAt.Time) {
				maximumStartedAt = containerState.StartedAt
			}
		}
	}
	if maximumStartedAt == nil {
		return nil
	}
	if runningContainerCount > restartGroup.Spec.Size {
		return fmt.Errorf("number of running containers is bigger than total number of containers")
	}
	if runningContainerCount < restartGroup.Spec.Size {
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

func (r *RestartGroupReconciler) updateBroadcastConfigMap(ctx context.Context, restartGroup *jobset.RestartGroup) error {
	log := ctrl.LoggerFrom(ctx)

	// No need to create / update broadcast ConfigMap if workload has never restarted
	if restartGroup.Status.RestartStartedAt == nil || restartGroup.Status.RestartStartedAt.IsZero() {
		return nil
	}
	desiredData := map[string]string{
		jobset.RestartStartedAtDataKey: restartGroup.Status.RestartStartedAt.Format(time.RFC3339Nano),
	}

	// Get broadcast ConfigMap
	var childConfigMaps corev1.ConfigMapList
	if err := r.List(ctx, &childConfigMaps, client.InNamespace(restartGroup.Namespace), client.MatchingFields{ConfigMapOwnerKey: restartGroup.Name}); err != nil {
		return err
	}
	if len(childConfigMaps.Items) > 1 {
		return fmt.Errorf("expected 0 or 1 child ConfigMap, but found %d", len(childConfigMaps.Items))
	}

	// Create broadcast ConfigMap if it doesn't exist
	if len(childConfigMaps.Items) == 0 {
		BroadcastConfigMapName := getBroadcastConfigMapName(restartGroup)
		broadcastConfigMap := &corev1.ConfigMap{
			ObjectMeta: metav1.ObjectMeta{
				Name:      BroadcastConfigMapName,
				Namespace: restartGroup.Namespace,
				Labels: map[string]string{
					jobset.RestartGroupNameKey: restartGroup.Name,
				},
			},
			Data: desiredData,
		}
		if err := ctrl.SetControllerReference(restartGroup, broadcastConfigMap, r.Scheme); err != nil {
			return fmt.Errorf("setting controller reference: %w", err)
		}
		log.V(2).Info("Creating broadcast configmap", "configmap", klog.KObj(broadcastConfigMap))
		if err := r.Create(ctx, broadcastConfigMap); err != nil {
			return fmt.Errorf("creating broadcast configmap: %w", err)
		}
	}

	broadcastConfigMap := childConfigMaps.Items[0]

	// Skip update if nothing changed
	if currentRestartStartedAt, ok := broadcastConfigMap.Data[jobset.RestartStartedAtDataKey]; ok && currentRestartStartedAt == desiredData[jobset.RestartStartedAtDataKey] {
		return nil
	}

	// Update broadcast ConfigMap
	broadcastConfigMap.Data = desiredData
	log.V(2).Info("Updating broadcast configmap", "configmap", klog.KObj(&broadcastConfigMap))
	if err := r.Update(ctx, &broadcastConfigMap); err != nil {
		return fmt.Errorf("updating broadcast configmap: %w", err)
	}
	return nil
}

func getBroadcastConfigMapName(restartGroup *jobset.RestartGroup) string {
	return restartGroup.Name + "-broadcast"
}
