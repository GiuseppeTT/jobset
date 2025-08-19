package main

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"os"
	"time"

	v1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/apimachinery/pkg/watch"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/rest"
)

const (
	podNamespaceKey         = "POD_NAMESPACE"
	podNameKey              = "POD_NAME"
	barrierStartedAtKey     = "alpha.jobset.sigs.k8s.io/barrier-started-at"
	liftBarrierStartedAtKey = "alpha.jobset.sigs.k8s.io/lift-barrier-started-at"
)

func main() {
	runBarrier()
	runWorker()
}

func runBarrier() {
	log.Printf("INFO: Starting barrier")
	kubernetesClient := getKubernetesClient()
	podNamespace, podName := getEnvironmentVariables()
	setBarrierStartedAtAnnotation(kubernetesClient, podNamespace, podName)
	waitAtBarrier(kubernetesClient, podNamespace, podName)
}

func getKubernetesClient() *kubernetes.Clientset {
	config, err := rest.InClusterConfig()
	if err != nil {
		log.Fatalf("ERROR: Failed to get in-cluster config: %v", err)
	}
	kubernetesClient, err := kubernetes.NewForConfig(config)
	if err != nil {
		log.Fatalf("ERROR: Failed to create Kubernetes client: %v", err)
	}
	return kubernetesClient
}

func getEnvironmentVariables() (string, string) {
	podNamespace := os.Getenv(podNamespaceKey)
	if podNamespace == "" {
		log.Fatalf("ERROR: '%s' environment variable must be set", podNamespaceKey)
	}
	podName := os.Getenv(podNameKey)
	if podName == "" {
		log.Fatalf("ERROR: '%s' environment variable must be set", podNameKey)
	}
	return podNamespace, podName
}

func setBarrierStartedAtAnnotation(kubernetesClient *kubernetes.Clientset, podNamespace string, podName string) {
	now := time.Now().UTC().Format(time.RFC3339)
	log.Printf("DEBUG: Setting '%s' annotation to '%s'", barrierStartedAtKey, now)
	patch := map[string]any{
		"metadata": map[string]any{
			"annotations": map[string]string{
				barrierStartedAtKey: now,
			},
		},
	}
	patchBytes, err := json.Marshal(patch)
	if err != nil {
		log.Fatalf("ERROR: Failed to marshal patch: %v", err)
	}
	_, err = kubernetesClient.CoreV1().Pods(podNamespace).Patch(context.TODO(), podName, types.MergePatchType, patchBytes, metav1.PatchOptions{})
	if err != nil {
		log.Fatalf("ERROR: Failed to patch Pod '%s/%s': %v", podNamespace, podName, err)
	}
}

func waitAtBarrier(kubernetesClient *kubernetes.Clientset, podNamespace string, podName string) {
	ownPodEventChannel := getOwnPodEventChannel(kubernetesClient, podNamespace, podName)
	for event := range ownPodEventChannel {
		pod, ok := event.Object.(*v1.Pod)
		if !ok {
			log.Fatalf("ERROR: Unexpected object type: %T", event.Object)
		}
		barrierStartedAt, err := getBarrierStartedAt(pod)
		if err != nil {
			log.Printf("WARN: Missing barrier started at: %v", err)
			continue
		}
		liftBarrierStartedAt, err := getLiftBarrierStartedAt(pod)
		if err != nil {
			log.Printf("WARN: Missing lift barrier started at: %v", err)
			continue
		}
		log.Printf("DEBUG: barrierStartedAt=%s", barrierStartedAt)
		log.Printf("DEBUG: liftBarrierStartedAt=%s", liftBarrierStartedAt)
		if barrierStartedAt.Before(liftBarrierStartedAt) || barrierStartedAt.Equal(liftBarrierStartedAt) {
			log.Printf("INFO: Lifting barrier")
			removeBarrierStartedAtAnnotation(kubernetesClient, podNamespace, podName)
			return
		}
	}
}

func getOwnPodEventChannel(kubernetesClient *kubernetes.Clientset, podNamespace string, podName string) <-chan watch.Event {
	watcher, err := kubernetesClient.CoreV1().Pods(podNamespace).Watch(context.TODO(), metav1.ListOptions{
		FieldSelector: fmt.Sprintf("metadata.name=%s", podName),
	})
	if err != nil {
		log.Fatalf("ERROR: Failed to watch own Pod '%s/%s': %v", podNamespace, podName, err)
	}
	return watcher.ResultChan()
}

func getBarrierStartedAt(pod *v1.Pod) (time.Time, error) {
	barrierStartedAtRaw, ok := pod.Annotations[barrierStartedAtKey]
	if !ok {
		return time.Time{}, fmt.Errorf("missing '%s' annotation", barrierStartedAtKey)
	}
	barrierStartedAt, err := time.Parse(time.RFC3339, barrierStartedAtRaw)
	if err != nil {
		return time.Time{}, fmt.Errorf("failed to parse '%s' from annotation '%s'", barrierStartedAtRaw, barrierStartedAtKey)
	}
	return barrierStartedAt, nil
}

func getLiftBarrierStartedAt(pod *v1.Pod) (time.Time, error) {
	liftBarrierStartedAtRaw, ok := pod.Annotations[liftBarrierStartedAtKey]
	if !ok {
		return time.Time{}, fmt.Errorf("missing '%s' annotation", liftBarrierStartedAtKey)
	}
	liftBarrierStartedAt, err := time.Parse(time.RFC3339, liftBarrierStartedAtRaw)
	if err != nil {
		return time.Time{}, fmt.Errorf("failed to parse '%s' from annotation '%s'", liftBarrierStartedAtRaw, liftBarrierStartedAtKey)
	}
	return liftBarrierStartedAt, nil
}

func removeBarrierStartedAtAnnotation(kubernetesClient *kubernetes.Clientset, podNamespace string, podName string) {
	log.Printf("DEBUG: Removing '%s' annotation", barrierStartedAtKey)
	patch := map[string]any{
		"metadata": map[string]any{
			"annotations": map[string]*string{
				barrierStartedAtKey: nil,
			},
		},
	}
	patchBytes, err := json.Marshal(patch)
	if err != nil {
		log.Fatalf("ERROR: Failed to marshal patch for removing annotation: %v", err)
	}
	_, err = kubernetesClient.CoreV1().Pods(podNamespace).Patch(context.TODO(), podName, types.MergePatchType, patchBytes, metav1.PatchOptions{})
	if err != nil {
		log.Fatalf("ERROR: Failed to patch Pod '%s/%s' to remove annotation: %v", podNamespace, podName, err)
	}
}

func runWorker() {
	log.Printf("INFO: Starting worker")
	watchForExitFiles()
}

func watchForExitFiles() {
	for {
		if _, err := os.Stat("/tmp/succeed"); err == nil {
			log.Printf("INFO: Worker finished successfully, exiting with 0")
			os.Exit(0)
		} else if !os.IsNotExist(err) {
			log.Printf("ERROR: Failed to stat /tmp/succeed: %v", err)
		}
		if _, err := os.Stat("/tmp/fail"); err == nil {
			log.Printf("INFO: Worker failed, exiting with 1")
			os.Exit(1)
		} else if !os.IsNotExist(err) {
			log.Printf("ERROR: Failed to stat /tmp/fail: %v", err)
		}
		time.Sleep(1 * time.Second)
	}
}
