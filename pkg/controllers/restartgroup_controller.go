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
	log.V(4).Info("Reconciling RestartGroup") // Set to V(4) because any Pod update will trigger it

	var managedPods corev1.PodList
	if err := r.getManagedPods(ctx, restartGroup, &managedPods); err != nil {
		log.Error(err, "getting managed pods")
		return ctrl.Result{}, err
	}

	if err := r.updateRestartGroupStatus(ctx, &restartGroup); apierrors.IsConflict(err) {
		log.Error(err, "updating restart group status")
		return ctrl.Result{Requeue: true}, nil
	}

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
