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
	"maps"
	"time"

	corev1 "k8s.io/api/core/v1"
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

// SetupWithManager sets up the controller with the Manager
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
	var restartGroup jobset.RestartGroup
	if err := r.getReconciledRestartGroup(ctx, req, &restartGroup); err != nil {
		return ctrl.Result{}, client.IgnoreNotFound(err)
	}

	log := ctrl.LoggerFrom(ctx).WithValues("restartgroup", klog.KObj(&restartGroup))
	ctx = ctrl.LoggerInto(ctx, log)
	log.V(2).Info("Reconciling RestartGroup")

	var managedPods corev1.PodList
	if err := r.getManagedPods(ctx, restartGroup, &managedPods); err != nil {
		log.Error(err, "getting managed pods")
		return ctrl.Result{}, err
	}

	currentWorkerStatuses := getWorkerStatuses(restartGroup, managedPods)

	// If all workers are running, great, no op
	if allWorkersAreRunning(restartGroup, currentWorkerStatuses) {
		log.V(2).Info("All workers are running")
		return ctrl.Result{}, nil
	}

	// If all workers are waiting, make sure lift state and broadcast configmap are updated
	if allWorkersAreWaiting(restartGroup, currentWorkerStatuses) {
		log.V(2).Info("All workers are waiting")
		currentLiftBarrierStartedBeforeOrAt := getLiftBarrierStartedBeforeOrAt(currentWorkerStatuses)
		if err := r.updateLiftBarrierStartedBeforeOrAtState(ctx, &restartGroup, currentLiftBarrierStartedBeforeOrAt); err != nil {
			log.Error(err, "updating lift barrier started before or at state")
			return ctrl.Result{}, err
		}
		if err := r.broadcastConfigMap(ctx, &restartGroup); err != nil {
			log.Error(err, "broadcasting ConfigMap")
			return ctrl.Result{}, err
		}
		return ctrl.Result{}, nil
	}

	// If new barrier, make sure restart state and broadcast configmap are updated
	if newBarrier(restartGroup, currentWorkerStatuses) {
		log.V(2).Info("New barrier")
		currentRestartWorkerStartedBeforeOrAt := getRestartWorkerStartedBeforeOrAt(restartGroup, currentWorkerStatuses)
		log.V(2).Info("DEBUG Value", "currentRestartWorkerStartedBeforeOrAt", currentRestartWorkerStartedBeforeOrAt)
		if err := r.updateRestartWorkerStartedBeforeOrAtState(ctx, &restartGroup, currentRestartWorkerStartedBeforeOrAt); err != nil {
			log.Error(err, "updating restart worker started before or at state")
			return ctrl.Result{}, err
		}
		if err := r.broadcastConfigMap(ctx, &restartGroup); err != nil {
			log.Error(err, "broadcasting ConfigMap")
			return ctrl.Result{}, err
		}
		return ctrl.Result{}, nil
	}

	log.V(2).Info("No case")

	return ctrl.Result{}, nil
}

func (r *RestartGroupReconciler) getReconciledRestartGroup(ctx context.Context, req ctrl.Request, restartGroup *jobset.RestartGroup) error {
	return r.Get(ctx, req.NamespacedName, restartGroup)
}

func (r *RestartGroupReconciler) getManagedPods(ctx context.Context, restartGroup jobset.RestartGroup, managedPods *corev1.PodList) error {
	return r.List(ctx, managedPods, client.InNamespace(restartGroup.Namespace), client.MatchingFields{PodRestartGroupNameKey: restartGroup.Name})
}

type WorkerStatus struct {
	BarrierStartedAt *metav1.Time
	WorkerStartedAt  *metav1.Time
}

func NewWorkerStatus(barrierStartedAt *metav1.Time, workerStartedAt *metav1.Time) (WorkerStatus, error) {
	if barrierStartedAt == nil && workerStartedAt == nil {
		return WorkerStatus{}, fmt.Errorf("both barrierStartedAt and workerStartedAt are nil")
	}
	if barrierStartedAt != nil && workerStartedAt != nil {
		return WorkerStatus{}, fmt.Errorf("both barrierStartedAt and workerStartedAt are not nil")
	}
	return WorkerStatus{
		BarrierStartedAt: barrierStartedAt,
		WorkerStartedAt:  workerStartedAt,
	}, nil
}

func (w WorkerStatus) IsWaiting() bool {
	return w.BarrierStartedAt != nil && w.WorkerStartedAt == nil
}

func (w WorkerStatus) IsRunning() bool {
	return w.BarrierStartedAt == nil && w.WorkerStartedAt != nil
}

func getWorkerStatuses(restartGroup jobset.RestartGroup, managedPods corev1.PodList) map[string]WorkerStatus {
	workerStatuses := make(map[string]WorkerStatus, restartGroup.Spec.Size)
	for _, pod := range managedPods.Items {
		id := pod.GenerateName
		for _, containerStatus := range pod.Status.ContainerStatuses {
			if containerStatus.Name == restartGroup.Spec.Container {
				if containerStatus.State.Running == nil {
					break
				}
				if rawWorkerStartedAt, ok := pod.Annotations[jobset.WorkerStartedAtKey]; ok {
					workerStartedAt, err := time.Parse(time.RFC3339, rawWorkerStartedAt)
					if err == nil && (workerStartedAt.After(containerStatus.State.Running.StartedAt.Time) || workerStartedAt.Equal(containerStatus.State.Running.StartedAt.Time)) {
						workerStartedAtValue := metav1.NewTime(workerStartedAt)
						workerStatus, err := NewWorkerStatus(nil, &workerStartedAtValue)
						if err != nil {
							break
						}
						workerStatuses[id] = workerStatus
						break
					}
				}
				workerStatus, err := NewWorkerStatus(&containerStatus.State.Running.StartedAt, nil)
				if err != nil {
					break
				}
				workerStatuses[id] = workerStatus
				break
			}
		}
	}
	return workerStatuses
}

func allWorkersAreRunning(restartGroup jobset.RestartGroup, currentWorkerStatuses map[string]WorkerStatus) bool {
	runningWorkerCount := 0
	for _, currentWorkerStatus := range currentWorkerStatuses {
		if currentWorkerStatus.IsRunning() {
			runningWorkerCount++
		}
	}
	return runningWorkerCount == int(restartGroup.Spec.Size)
}

func allWorkersAreWaiting(restartGroup jobset.RestartGroup, currentWorkerStatuses map[string]WorkerStatus) bool {
	workerWaitingCount := 0
	for _, currentWorkerStatus := range currentWorkerStatuses {
		if currentWorkerStatus.IsWaiting() {
			workerWaitingCount++
		}
	}
	return workerWaitingCount == int(restartGroup.Spec.Size)
}

func getLiftBarrierStartedBeforeOrAt(currentWorkerStatuses map[string]WorkerStatus) *metav1.Time {
	var liftBarrierStartedBeforeOrAt *metav1.Time
	for _, currentWorkerStatus := range currentWorkerStatuses {
		if currentWorkerStatus.IsWaiting() {
			if liftBarrierStartedBeforeOrAt == nil || liftBarrierStartedBeforeOrAt.Before(currentWorkerStatus.BarrierStartedAt) {
				liftBarrierStartedBeforeOrAt = currentWorkerStatus.BarrierStartedAt
			}
		}
	}
	return liftBarrierStartedBeforeOrAt
}

func (r *RestartGroupReconciler) updateLiftBarrierStartedBeforeOrAtState(ctx context.Context, restartGroup *jobset.RestartGroup, liftBarrierStartedBeforeOrAt *metav1.Time) error {
	log := ctrl.LoggerFrom(ctx)
	if restartGroup.Status.LiftBarrierStartedBeforeOrAt.Equal(liftBarrierStartedBeforeOrAt) {
		return nil
	}
	log.V(2).Info("Updating lift barrier started before or at state", "liftBarrierStartedBeforeOrAt", liftBarrierStartedBeforeOrAt)
	restartGroup.Status.LiftBarrierStartedBeforeOrAt = liftBarrierStartedBeforeOrAt
	return r.Status().Update(ctx, restartGroup)
}

func (r *RestartGroupReconciler) broadcastConfigMap(ctx context.Context, restartGroup *jobset.RestartGroup) error {
	log := ctrl.LoggerFrom(ctx)

	desiredData := map[string]string{}
	if restartGroup.Status.LiftBarrierStartedBeforeOrAt != nil {
		desiredData[jobset.LiftBarrierStartedBeforeOrAtKey] = restartGroup.Status.LiftBarrierStartedBeforeOrAt.Format(time.RFC3339)
	}
	if restartGroup.Status.RestartWorkerStartedBeforeOrAt != nil {
		desiredData[jobset.RestartWorkerStartedBeforeOrAtKey] = restartGroup.Status.RestartWorkerStartedBeforeOrAt.Format(time.RFC3339)
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
		broadcastConfigMapName := getBroadcastConfigMapName(restartGroup)
		broadcastConfigMapNamespace := restartGroup.Namespace
		broadcastConfigMapLabels := maps.Clone(restartGroup.Labels)
		broadcastConfigMapLabels[jobset.RestartGroupNameKey] = restartGroup.Name
		broadcastConfigMap := corev1.ConfigMap{
			ObjectMeta: metav1.ObjectMeta{
				Name:      broadcastConfigMapName,
				Namespace: broadcastConfigMapNamespace,
				Labels:    broadcastConfigMapLabels,
			},
			Data: desiredData,
		}
		if err := ctrl.SetControllerReference(restartGroup, &broadcastConfigMap, r.Scheme); err != nil {
			return fmt.Errorf("setting controller reference: %w", err)
		}
		log.V(2).Info("Creating broadcast ConfigMap", "configmap", klog.KObj(&broadcastConfigMap))
		if err := r.Create(ctx, &broadcastConfigMap); err != nil {
			return fmt.Errorf("creating broadcast ConfigMap: %w", err)
		}
		return nil
	}

	broadcastConfigMap := childConfigMaps.Items[0]

	// Skip update if nothing changed
	if maps.Equal(broadcastConfigMap.Data, desiredData) {
		return nil
	}

	// Update broadcast ConfigMap
	broadcastConfigMap.Data = desiredData
	log.V(2).Info("Updating broadcast ConfigMap", "configmap", klog.KObj(&broadcastConfigMap))
	if err := r.Update(ctx, &broadcastConfigMap); err != nil {
		return fmt.Errorf("updating broadcast ConfigMap: %w", err)
	}
	return nil
}

func getBroadcastConfigMapName(restartGroup *jobset.RestartGroup) string {
	return restartGroup.Name + "-broadcast"
}

func newBarrier(restartGroup jobset.RestartGroup, currentWorkerStatuses map[string]WorkerStatus) bool {
	if restartGroup.Status.LiftBarrierStartedBeforeOrAt == nil {
		return false
	}
	currentLiftBarrierStartedBeforeOrAt := getLiftBarrierStartedBeforeOrAt(currentWorkerStatuses)
	if currentLiftBarrierStartedBeforeOrAt == nil {
		return false
	}
	return currentLiftBarrierStartedBeforeOrAt.After(restartGroup.Status.LiftBarrierStartedBeforeOrAt.Time)
}

func getRestartWorkerStartedBeforeOrAt(restartGroup jobset.RestartGroup, currentWorkerStatuses map[string]WorkerStatus) *metav1.Time {
	var restartWorkerStartedBeforeOrAt *metav1.Time
	for _, currentWorkerStatus := range currentWorkerStatuses {
		if currentWorkerStatus.IsRunning() {
			if restartWorkerStartedBeforeOrAt == nil || restartWorkerStartedBeforeOrAt.Before(currentWorkerStatus.WorkerStartedAt) {
				restartWorkerStartedBeforeOrAt = currentWorkerStatus.WorkerStartedAt
			}
		}
	}
	if restartGroup.Status.RestartWorkerStartedBeforeOrAt != nil && (restartWorkerStartedBeforeOrAt == nil || restartWorkerStartedBeforeOrAt.Before(restartGroup.Status.RestartWorkerStartedBeforeOrAt)) {
		restartWorkerStartedBeforeOrAt = restartGroup.Status.RestartWorkerStartedBeforeOrAt
	}
	return restartWorkerStartedBeforeOrAt
}

func (r *RestartGroupReconciler) updateRestartWorkerStartedBeforeOrAtState(ctx context.Context, restartGroup *jobset.RestartGroup, restartWorkerStartedBeforeOrAt *metav1.Time) error {
	log := ctrl.LoggerFrom(ctx)
	if restartGroup.Status.RestartWorkerStartedBeforeOrAt != nil && restartGroup.Status.RestartWorkerStartedBeforeOrAt.Equal(restartWorkerStartedBeforeOrAt) {
		return nil
	}
	log.V(2).Info("Updating restart worker started before or at state", "restartWorkerStartedBeforeOrAt", restartWorkerStartedBeforeOrAt)
	restartGroup.Status.RestartWorkerStartedBeforeOrAt = restartWorkerStartedBeforeOrAt
	return r.Status().Update(ctx, restartGroup)
}
