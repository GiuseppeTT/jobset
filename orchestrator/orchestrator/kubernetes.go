package orchestrator

import (
	"context"
	"fmt"
	"time"

	batchv1 "k8s.io/api/batch/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/rest"
	"k8s.io/klog/v2"
	jobset "sigs.k8s.io/jobset/api/jobset/v1alpha2"
	jobsetclientset "sigs.k8s.io/jobset/client-go/clientset/versioned"
)

const (
	kubeJobSetUidLabelKey                     = "jobset.sigs.k8s.io/jobset-uid" // TODO: Get from JobSet go client
	kubeInPlacePodRestartTimeoutAnnotationKey = "jobset.sigs.k8s.io/in-place-pod-restart-timeout"
)

func connectToKubernetes() (*kubernetes.Clientset, *jobsetclientset.Clientset, error) {
	klog.Info("Connecting to Kubernetes")
	config, err := rest.InClusterConfig()
	if err != nil {
		return nil, nil, fmt.Errorf("failed to get in cluster config: %w", err)
	}
	kubeClient, err := kubernetes.NewForConfig(config)
	if err != nil {
		return nil, nil, fmt.Errorf("failed to create Kubernetes client: %w", err)
	}
	jobsetClient, err := jobsetclientset.NewForConfig(config)
	if err != nil {
		return nil, nil, fmt.Errorf("failed to create JobSet client: %w", err)
	}
	return kubeClient, jobsetClient, nil
}

func (o *Orchestrator) listJobSetsFromKube(ctx context.Context) ([]string, map[string]jobset.JobSet, error) {
	klog.V(2).Info("Listing JobSets from k8s")
	jobSetList, err := o.jobsetClient.JobsetV1alpha2().JobSets("").List(ctx, metav1.ListOptions{})
	if err != nil {
		return []string{}, map[string]jobset.JobSet{}, fmt.Errorf("failed to list JobSets: %w", err)
	}
	jobSetUids := []string{}
	jobSetByUid := map[string]jobset.JobSet{}
	for _, jobSet := range jobSetList.Items {
		_, containsAnnotation := jobSet.Annotations[kubeInPlacePodRestartTimeoutAnnotationKey]
		if containsAnnotation {
			jobSetUids = append(jobSetUids, string(jobSet.UID))
			jobSetByUid[string(jobSet.UID)] = jobSet
		}
	}
	return jobSetUids, jobSetByUid, nil
}

func (o *Orchestrator) deleteChildJobs(ctx context.Context, jobSetUid string) error {
	klog.Infof("Deleting child jobs. jobSetUid=%s", jobSetUid)
	childJobs, err := o.listChildJobs(ctx, jobSetUid)
	if err != nil {
		return fmt.Errorf("failed to list child jobs: %w", err)
	}
	foregroundPolicy := metav1.DeletePropagationForeground
	for _, childJob := range childJobs.Items {
		err := o.kubeClient.BatchV1().Jobs(childJob.Namespace).Delete(ctx, childJob.Name, metav1.DeleteOptions{PropagationPolicy: &foregroundPolicy})
		if err != nil {
			klog.Errorf("Failed to delete child job '%s' in namespace '%s' of JobSet UID '%s': %v", childJob.Name, childJob.Namespace, jobSetUid, err)
		}
	}
	return nil
}

func (o *Orchestrator) listChildJobs(ctx context.Context, jobSetUid string) (*batchv1.JobList, error) {
	labelSelector := fmt.Sprintf("%s=%s", kubeJobSetUidLabelKey, jobSetUid)
	childJobs, err := o.kubeClient.BatchV1().Jobs("").List(ctx, metav1.ListOptions{LabelSelector: labelSelector})
	if err != nil {
		return &batchv1.JobList{}, fmt.Errorf("failed to list child jobs of JobSet UID '%s': %w", jobSetUid, err)
	}
	return childJobs, nil
}

func (o *Orchestrator) countWorkers(jobSet jobset.JobSet) int {
	workerCount := 0
	for _, rjob := range jobSet.Spec.ReplicatedJobs {
		workerCount += int(rjob.Replicas) * int(*rjob.Template.Spec.Parallelism)
	}
	return workerCount
}

func (o *Orchestrator) getTimeoutDuration(jobSet jobset.JobSet) (time.Duration, error) {
	rawTimeout, ok := jobSet.Annotations[kubeInPlacePodRestartTimeoutAnnotationKey]
	if !ok {
		return time.Duration(0), fmt.Errorf("in-place-pod-restart-timeout annotation not found")
	}
	timeout, err := time.ParseDuration(rawTimeout)
	if err != nil {
		return time.Duration(0), fmt.Errorf("failed to parse in-place-pod-restart-timeout annotation: %w", err)
	}
	return timeout, nil
}
