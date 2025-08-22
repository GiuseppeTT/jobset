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
	podNamespaceKey                 = "POD_NAMESPACE"
	podNameKey                      = "POD_NAME"
	barrierStartedAtKey             = "alpha.jobset.sigs.k8s.io/barrier-started-at"
	liftBarrierStartedBeforeOrAtKey = "liftBarrierStartedBeforeOrAt"
)

func main() {
	runBarrier()
	runWorker()
}

func runBarrier() {
	log.Printf("INFO: Starting barrier")
	kubernetesClient := getKubernetesClient()
	podNamespace, podName, restartGroupName := getEnvironmentVariables()
	barrierStartedAt := setBarrierStartedAtAnnotation(kubernetesClient, podNamespace, podName)
	waitAtBarrier(kubernetesClient, podNamespace, podName, restartGroupName, barrierStartedAt)
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

func getEnvironmentVariables() (string, string, string) {
	podNamespace := os.Getenv(podNamespaceKey)
	if podNamespace == "" {
		log.Fatalf("ERROR: '%s' environment variable must be set", podNamespaceKey)
	}
	podName := os.Getenv(podNameKey)
	if podName == "" {
		log.Fatalf("ERROR: '%s' environment variable must be set", podNameKey)
	}
	restartGroupName := os.Getenv("RESTART_GROUP_NAME")
	if restartGroupName == "" {
		log.Fatalf("ERROR: 'RESTART_GROUP_NAME' env var must be set")
	}
	return podNamespace, podName, restartGroupName

}

func setBarrierStartedAtAnnotation(kubernetesClient *kubernetes.Clientset, podNamespace string, podName string) time.Time {
	barrierStartedAt := time.Now().Truncate(time.Second).UTC()
	rawBarrierStartedAt := barrierStartedAt.Format(time.RFC3339)
	log.Printf("DEBUG: Setting '%s' annotation to '%s'", barrierStartedAtKey, rawBarrierStartedAt)
	patch := map[string]any{
		"metadata": map[string]any{
			"annotations": map[string]string{
				barrierStartedAtKey: rawBarrierStartedAt,
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
	return barrierStartedAt
}

func waitAtBarrier(kubernetesClient *kubernetes.Clientset, podNamespace string, podName string, restartGroupName string, barrierStartedAt time.Time) {
	broadcastConfigMapEventChannel := getBroadcastConfigMapEventChannel(kubernetesClient, podNamespace, restartGroupName)
	for event := range broadcastConfigMapEventChannel {
		liftBarrierStartedBeforeOrAt, err := getLiftBarrierStartedBeforeOrAt(event)
		if err != nil {
			log.Printf("ERROR: Failed to get lift barrier started before or at timestamp: %v", err)
			continue
		}
		log.Printf("DEBUG: barrierStartedAt: %s", barrierStartedAt)
		log.Printf("DEBUG: liftBarrierStartedBeforeOrAt: %s", liftBarrierStartedBeforeOrAt)
		if barrierStartedAt.Before(liftBarrierStartedBeforeOrAt) || barrierStartedAt.Equal(liftBarrierStartedBeforeOrAt) {
			log.Printf("INFO: Lifting barrier")
			removeBarrierStartedAtAnnotation(kubernetesClient, podNamespace, podName)
			return
		}
	}
}

func getBroadcastConfigMapEventChannel(kubernetesClient *kubernetes.Clientset, podNamespace string, restartGroupName string) <-chan watch.Event {
	broadcastConfigMapName := restartGroupName + "-broadcast"
	watcher, err := kubernetesClient.CoreV1().ConfigMaps(podNamespace).Watch(context.TODO(), metav1.ListOptions{
		FieldSelector: fmt.Sprintf("metadata.name=%s", broadcastConfigMapName),
	})
	if err != nil {
		log.Fatalf("ERROR: Failed to watch broadcast ConfigMap '%s': %v", broadcastConfigMapName, err)
	}
	return watcher.ResultChan()
}

func getLiftBarrierStartedBeforeOrAt(event watch.Event) (time.Time, error) {
	configMap, ok := event.Object.(*v1.ConfigMap)
	if !ok {
		return time.Time{}, fmt.Errorf("unexpected object type: %T", event.Object)
	}
	rawliftBarrierStartedBeforeOrAt, ok := configMap.Data[liftBarrierStartedBeforeOrAtKey]
	if !ok {
		return time.Time{}, fmt.Errorf("'%s' not found in ConfigMap data", liftBarrierStartedBeforeOrAtKey)
	}
	liftBarrierStartedBeforeOrAt, err := time.Parse(time.RFC3339, rawliftBarrierStartedBeforeOrAt)
	if err != nil {
		return time.Time{}, fmt.Errorf("failed to parse lift barrier started before or at timestamp: %v", err)
	}
	return liftBarrierStartedBeforeOrAt, nil
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
