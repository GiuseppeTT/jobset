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
	"errors"
	"fmt"
	"sync"
	"time"

	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/tools/record"
	"k8s.io/client-go/util/workqueue"
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

	return nil
}

// +kubebuilder:rbac:groups=jobset.x-k8s.io,resources=restartgroups,verbs=get;list;watch;create;update;patch;delete
// +kubebuilder:rbac:groups=jobset.x-k8s.io,resources=restartgroups/status,verbs=get;update;patch
// +kubebuilder:rbac:groups=jobset.x-k8s.io,resources=restartgroups/finalizers,verbs=update

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

	log.V(2).Info("Getting managed Pods")
	var managedPods corev1.PodList
	if err := r.getManagedPods(ctx, restartGroup, &managedPods); err != nil {
		log.Error(err, "getting managed pods")
		return ctrl.Result{}, err
	}

	log.V(2).Info("Getting current worker states")
	currentWorkerStatuses := getWorkerStatuses(restartGroup, managedPods)

	log.V(2).Info("Calculating new state")
	if err := r.reconcile(ctx, &restartGroup, currentWorkerStatuses, managedPods); err != nil {
		log.Error(err, "reconciling restart group")
		return ctrl.Result{}, err
	}

	log.V(2).Info("Updating RestartGroup status")
	if err := r.updateRestartGroupStatus(ctx, &restartGroup); apierrors.IsConflict(err) {
		log.Error(err, "updating restart group status")
		return ctrl.Result{Requeue: true}, nil
	}

	log.V(2).Info("Lifting barrier")
	if err := r.liftBarriers(ctx, restartGroup, managedPods); err != nil {
		log.Error(err, "lifting barrier")
		return ctrl.Result{}, err
	}

	log.V(2).Info("Finishing RestartGroup reconcile")
	return ctrl.Result{}, nil
}

func (r *RestartGroupReconciler) getReconciledRestartGroup(ctx context.Context, req ctrl.Request, restartGroup *jobset.RestartGroup) error {
	return r.Get(ctx, req.NamespacedName, restartGroup)
}

func (r *RestartGroupReconciler) getManagedPods(ctx context.Context, restartGroup jobset.RestartGroup, managedPods *corev1.PodList) error {
	return r.List(ctx, managedPods, client.InNamespace(restartGroup.Namespace), client.MatchingFields{PodRestartGroupNameKey: restartGroup.Name})
}

func (r *RestartGroupReconciler) updateRestartGroupStatus(ctx context.Context, restartGroup *jobset.RestartGroup) error {
	return r.Status().Update(ctx, restartGroup)
}

type InternalWorkerStatus struct {
	BarrierStartedAt *metav1.Time
	WorkerStartedAt  *metav1.Time
}

func NewInternalWorkerStatus(barrierStartedAt *metav1.Time, workerStartedAt *metav1.Time) (InternalWorkerStatus, error) {
	if barrierStartedAt == nil && workerStartedAt == nil {
		return InternalWorkerStatus{}, fmt.Errorf("both barrierStartedAt and workerStartedAt are nil")
	}
	if barrierStartedAt != nil && workerStartedAt != nil {
		return InternalWorkerStatus{}, fmt.Errorf("both barrierStartedAt and workerStartedAt are not nil")
	}
	return InternalWorkerStatus{
		BarrierStartedAt: barrierStartedAt,
		WorkerStartedAt:  workerStartedAt,
	}, nil
}

func (ws InternalWorkerStatus) IsWaiting() bool {
	return ws.BarrierStartedAt != nil && ws.WorkerStartedAt == nil
}

func (ws InternalWorkerStatus) IsRunning() bool {
	return ws.BarrierStartedAt == nil && ws.WorkerStartedAt != nil
}

func getWorkerStatuses(restartGroup jobset.RestartGroup, managedPods corev1.PodList) map[string]InternalWorkerStatus {
	workerStatuses := make(map[string]InternalWorkerStatus, restartGroup.Spec.Size)
	for _, pod := range managedPods.Items {
		id := pod.GenerateName
		if barrierStartedAtRaw, ok := pod.Annotations[jobset.BarrierStartedAtKey]; ok {
			barrierStartedAt, err := time.Parse(time.RFC3339, barrierStartedAtRaw)
			if err != nil {
				continue
			}
			barrierStartedAtValue := metav1.NewTime(barrierStartedAt)
			workerStatus, err := NewInternalWorkerStatus(&barrierStartedAtValue, nil)
			if err != nil {
				continue
			}
			workerStatuses[id] = workerStatus
			continue
		}
		for _, containerStatus := range pod.Status.ContainerStatuses {
			if containerStatus.Name == restartGroup.Spec.Container {
				if containerStatus.State.Running == nil {
					continue
				}
				workerStatus, err := NewInternalWorkerStatus(nil, &containerStatus.State.Running.StartedAt)
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

func (r *RestartGroupReconciler) reconcile(ctx context.Context, restartGroup *jobset.RestartGroup, currentWorkerStatuses map[string]InternalWorkerStatus, managedPods corev1.PodList) error {
	// All workers are running
	// Good, no op
	if allWorkersAreRunning(*restartGroup, currentWorkerStatuses) {
		return nil
	}
	// All workers are waiting at the barrier
	// Make sure saved barrier is current to lift it
	if allWorkersAreWaiting(*restartGroup, currentWorkerStatuses) {
		return saveBarrier(restartGroup, currentWorkerStatuses)
	}
	// There are workers running but one of the workers is in a new barrier
	// Must restart running workers
	if newBarrier(*restartGroup, currentWorkerStatuses) {
		return r.restartWorkers(ctx, currentWorkerStatuses, managedPods)
	}
	// Current barrier is still lifting
	// Workers are either running or in the saved barrier
	// Good, no op
	return nil
}

func allWorkersAreRunning(restartGroup jobset.RestartGroup, currentWorkerStatuses map[string]InternalWorkerStatus) bool {
	runningWorkerCount := 0
	for _, currentWorkerStatus := range currentWorkerStatuses {
		if currentWorkerStatus.IsRunning() {
			runningWorkerCount++
		}
	}
	return runningWorkerCount == int(restartGroup.Spec.Size)
}

func allWorkersAreWaiting(restartGroup jobset.RestartGroup, currentWorkerStatuses map[string]InternalWorkerStatus) bool {
	workerWaitingCount := 0
	for _, currentWorkerStatus := range currentWorkerStatuses {
		if currentWorkerStatus.IsWaiting() {
			workerWaitingCount++
		}
	}
	return workerWaitingCount == int(restartGroup.Spec.Size)
}

func saveBarrier(restartGroup *jobset.RestartGroup, currentWorkerStatuses map[string]InternalWorkerStatus) error {
	savedWorkerStatuses := make(map[string]jobset.WorkerStatus, restartGroup.Spec.Size)
	for id, currentWorkerStatus := range currentWorkerStatuses {
		savedWorkerStatuses[id] = jobset.WorkerStatus{
			BarrierStartedAt: currentWorkerStatus.BarrierStartedAt,
		}
	}
	restartGroup.Status.WorkerStatuses = savedWorkerStatuses
	return nil
}

func newBarrier(restartGroup jobset.RestartGroup, currentWorkerStatuses map[string]InternalWorkerStatus) bool {
	for id, currentWorkerStatus := range currentWorkerStatuses {
		if currentWorkerStatus.IsRunning() {
			continue
		}
		savedWorkerState, ok := restartGroup.Status.WorkerStatuses[id]
		if !ok {
			continue // Unexpected
		}
		if !currentWorkerStatus.BarrierStartedAt.Equal(savedWorkerState.BarrierStartedAt) {
			return true
		}
	}
	return false
}

func (r *RestartGroupReconciler) restartWorkers(ctx context.Context, currentWorkerStatuses map[string]InternalWorkerStatus, managedPods corev1.PodList) error {
	log := ctrl.LoggerFrom(ctx)
	var (
		lock      sync.Mutex
		finalErrs []error
	)
	workqueue.ParallelizeUntil(ctx, 500, len(managedPods.Items), func(i int) {
		pod := &managedPods.Items[i]
		id := pod.GenerateName
		currentWorkerStatus, ok := currentWorkerStatuses[id]
		if !ok || (ok && !currentWorkerStatus.IsRunning()) {
			return
		}
		if annotationRestartWorkerStartedAt, ok := pod.Annotations[jobset.RestartWorkerStartedAt]; ok && annotationRestartWorkerStartedAt == currentWorkerStatus.WorkerStartedAt.Format(time.RFC3339) {
			return
		}
		patch := client.MergeFrom(pod.DeepCopy())
		if pod.Annotations == nil {
			pod.Annotations = make(map[string]string)
		}
		pod.Annotations[jobset.RestartWorkerStartedAt] = currentWorkerStatus.WorkerStartedAt.Format(time.RFC3339)
		if err := r.Patch(ctx, pod, patch); err != nil {
			lock.Lock()
			defer lock.Unlock()
			log.Error(err, "patching pod with annotation", "pod", klog.KObj(pod))
			finalErrs = append(finalErrs, fmt.Errorf("patching pod %s: %w", pod.Name, err))
			return
		}
	})
	return errors.Join(finalErrs...)
}

func (r *RestartGroupReconciler) liftBarriers(ctx context.Context, restartGroup jobset.RestartGroup, managedPods corev1.PodList) error {
	log := ctrl.LoggerFrom(ctx)
	var (
		lock      sync.Mutex
		finalErrs []error
	)
	workqueue.ParallelizeUntil(ctx, 500, len(managedPods.Items), func(i int) {
		pod := &managedPods.Items[i]
		id := pod.GenerateName
		savedWorkerStatus, ok := restartGroup.Status.WorkerStatuses[id]
		if !ok {
			return
		}
		if annotationLiftBarrierStartedAt, ok := pod.Annotations[jobset.LiftBarrierStartedAtKey]; ok && annotationLiftBarrierStartedAt == savedWorkerStatus.BarrierStartedAt.Format(time.RFC3339) {
			return
		}
		patch := client.MergeFrom(pod.DeepCopy())
		if pod.Annotations == nil {
			pod.Annotations = make(map[string]string)
		}
		pod.Annotations[jobset.LiftBarrierStartedAtKey] = savedWorkerStatus.BarrierStartedAt.Format(time.RFC3339)
		if err := r.Patch(ctx, pod, patch); err != nil {
			lock.Lock()
			defer lock.Unlock()
			log.Error(err, "patching pod with annotation", "pod", klog.KObj(pod))
			finalErrs = append(finalErrs, fmt.Errorf("patching pod %s: %w", pod.Name, err))
			return
		}
	})
	return errors.Join(finalErrs...)
}
