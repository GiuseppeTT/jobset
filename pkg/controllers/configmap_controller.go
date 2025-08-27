package controllers

import (
	"context"
	"fmt"
	"math"
	"strconv"

	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/tools/record"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/predicate"
	jobset "sigs.k8s.io/jobset/api/jobset/v1alpha2"
)

type ConfigMapReconciler struct {
	client.Client
	Scheme *runtime.Scheme
	Record record.EventRecorder
}

func NewConfigMapReconciler(client client.Client, scheme *runtime.Scheme, record record.EventRecorder) *ConfigMapReconciler {
	return &ConfigMapReconciler{Client: client, Scheme: scheme, Record: record}
}

func (r *ConfigMapReconciler) SetupWithManager(mgr ctrl.Manager) error {
	return ctrl.NewControllerManagedBy(mgr).
		For(&corev1.ConfigMap{}).
		WithEventFilter(predicate.NewPredicateFuncs(func(object client.Object) bool {
			configMap, ok := object.(*corev1.ConfigMap)
			if !ok {
				return false
			}
			_, ok = configMap.GetLabels()[jobset.RestartGroupNameKey]
			return ok
		})).
		Complete(r)
}

func (r *ConfigMapReconciler) Reconcile(ctx context.Context, req ctrl.Request) (ctrl.Result, error) {
	log := ctrl.LoggerFrom(ctx).WithValues("generationConfigmap", req.NamespacedName)
	log.V(2).Info("Reconciling restart group ConfigMap")

	// Get generation ConfigMap
	log.V(2).Info("Getting generation ConfigMap")
	var generationConfigMap corev1.ConfigMap
	if err := r.Get(ctx, req.NamespacedName, &generationConfigMap); err != nil {
		return ctrl.Result{}, client.IgnoreNotFound(err)
	}
	restartGroupName, ok := generationConfigMap.GetLabels()[jobset.RestartGroupNameKey]
	if !ok {
		return ctrl.Result{}, fmt.Errorf("'%s' label missing in generation ConfigMap '%s'", jobset.RestartGroupNameKey, req.NamespacedName)
	}
	generationByWorkerId, err := getGenerationByWorkerId(generationConfigMap)
	if err != nil {
		return ctrl.Result{}, err
	}

	// Get JobSet
	log.V(2).Info("Getting JobSet")
	jobSetName, ok := generationConfigMap.GetLabels()[jobset.JobSetNameKey]
	if !ok {
		return ctrl.Result{}, fmt.Errorf("'%s' label missing in broadcast ConfigMap '%s'", jobset.JobSetNameKey, req.NamespacedName)
	}
	jobSetNamespacedName := types.NamespacedName{Name: jobSetName, Namespace: req.Namespace}
	var jobSet jobset.JobSet
	if err := r.Get(ctx, jobSetNamespacedName, &jobSet); err != nil {
		return ctrl.Result{}, client.IgnoreNotFound(err)
	}
	desiredWorkerCount, err := countWorkers(&jobSet)
	if err != nil {
		return ctrl.Result{}, err
	}
	log = log.WithValues("desiredWorkerCount", desiredWorkerCount)

	// Calculate new state
	log.V(2).Info("Calculating new state")
	if maximumGeneration, ok := allWorkersWaiting(generationByWorkerId, desiredWorkerCount); ok {
		log.V(2).Info("All workers are waiting in the new barrier")
		err := r.updateBroadcastConfigMap(ctx, req.Namespace, restartGroupName, maximumGeneration, -1)
		return ctrl.Result{}, err
	}
	if maximumGeneration, ok := newBarrier(generationByWorkerId); ok {
		log.V(2).Info("Only some workers are waiting in the new barrier")
		err := r.updateBroadcastConfigMap(ctx, req.Namespace, restartGroupName, -1, maximumGeneration-1)
		return ctrl.Result{}, err
	}

	return ctrl.Result{}, nil
}

func getGenerationByWorkerId(generationConfigMap corev1.ConfigMap) (map[string]int, error) {
	generationByWorkerId := make(map[string]int)
	for workerId, rawGeneration := range generationConfigMap.Data {
		generation, err := strconv.Atoi(rawGeneration)
		if err != nil {
			return map[string]int{}, fmt.Errorf("failed to parse generation '%s' for worker '%s': %v", rawGeneration, workerId, err)
		}
		generationByWorkerId[workerId] = generation
	}
	return generationByWorkerId, nil
}

func countWorkers(jobSet *jobset.JobSet) (int, error) {
	currentCount := 0
	for _, rjob := range jobSet.Spec.ReplicatedJobs {
		jobTemplate := rjob.Template
		if jobTemplate.Spec.Completions == nil || jobTemplate.Spec.Parallelism == nil || *jobTemplate.Spec.Completions != *jobTemplate.Spec.Parallelism {
			return 0, fmt.Errorf("in place restart requires jobTemplate.spec.completions == jobTemplate.spec.parallelism != nil")
		}
		currentCount += int(rjob.Replicas * *jobTemplate.Spec.Parallelism)
	}
	return currentCount, nil
}

func allWorkersWaiting(generationByWorkerId map[string]int, desiredWorkerCount int) (int, bool) {
	if len(generationByWorkerId) == 0 {
		return 0, false
	}
	minimum := math.MaxInt64
	maximum := math.MinInt64
	workerCount := 0
	for _, generation := range generationByWorkerId {
		if generation < minimum {
			minimum = generation
		}
		if generation > maximum {
			maximum = generation
		}
		workerCount++
	}
	return maximum, minimum == maximum && workerCount == desiredWorkerCount
}

func newBarrier(generationByWorkerId map[string]int) (int, bool) {
	if len(generationByWorkerId) == 0 {
		return 0, false
	}
	minimum := math.MaxInt64
	maximum := math.MinInt64
	for _, generation := range generationByWorkerId {
		if generation < minimum {
			minimum = generation
		}
		if generation > maximum {
			maximum = generation
		}
	}
	return maximum, minimum != maximum
}

func (r *ConfigMapReconciler) updateBroadcastConfigMap(ctx context.Context, namespace string, restartGroupName string, target int, restart int) error {
	var broadcastConfigMap corev1.ConfigMap
	namespacedName := types.NamespacedName{Namespace: namespace, Name: restartGroupName + "-broadcast"}
	if err := r.Get(ctx, namespacedName, &broadcastConfigMap); err != nil {
		return err
	}
	broadcastConfigMap.ManagedFields = nil
	if target >= 0 {
		broadcastConfigMap.Data["target"] = strconv.Itoa(target)
	}
	if restart >= 0 {
		broadcastConfigMap.Data["restart"] = strconv.Itoa(restart)
	}
	err := r.Patch(ctx, &broadcastConfigMap, client.Apply, client.FieldOwner("restartgroup-configmap-controller"), client.ForceOwnership)
	return err
}
