package main

import (
	"context"
	"encoding/json"
	"log"
	"os"
	"strconv"
	"time"

	v1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/apimachinery/pkg/watch"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/rest"
)

const (
	podNamespaceKey            = "POD_NAMESPACE"
	jobGlobalIndexKey          = "JOB_GLOBAL_INDEX"
	podIndexKey                = "POD_INDEX"
	generationConfigMapNameKey = "GENERATION_CONFIGMAP_NAME"
	broadcastConfigMapNameKey  = "BROADCAST_CONFIGMAP_NAME"
	targetKey                  = "target"
	restartKey                 = "restart"
	timeoutSeconds             = 1200
	pollIntervalSeconds        = 1
)

func main() {
	kubernetesClient := getKubernetesClient()
	podNamespace, jobGlobalIndex, podIndex, generationConfigMapName, broadcastConfigMapName := getEnvironmentVariables()
	workerId := jobGlobalIndex + "-" + podIndex
	generation := runBarrier(kubernetesClient, podNamespace, broadcastConfigMapName, generationConfigMapName, workerId)
	go runAgent(kubernetesClient, podNamespace, broadcastConfigMapName, generation)
	runWorker()
}

func getKubernetesClient() *kubernetes.Clientset {
	log.Printf("INFO: Creating Kubernetes client")
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

func getEnvironmentVariables() (string, string, string, string, string) {
	log.Printf("INFO: Getting environment variables")
	podNamespace := os.Getenv(podNamespaceKey)
	if podNamespace == "" {
		log.Fatalf("ERROR: '%s' environment variable must be set", podNamespaceKey)
	}
	log.Printf("DEBUG: podNamespace=%s", podNamespace)
	jobGlobalIndex := os.Getenv(jobGlobalIndexKey)
	if jobGlobalIndex == "" {
		log.Fatalf("ERROR: '%s' environment variable must be set", jobGlobalIndexKey)
	}
	log.Printf("DEBUG: jobGlobalIndex=%s", jobGlobalIndex)
	podIndex := os.Getenv(podIndexKey)
	if podIndex == "" {
		log.Fatalf("ERROR: '%s' environment variable must be set", podIndexKey)
	}
	log.Printf("DEBUG: podIndex=%s", podIndex)
	generationConfigMapName := os.Getenv(generationConfigMapNameKey)
	if generationConfigMapName == "" {
		log.Fatalf("ERROR: '%s' environment variable must be set", generationConfigMapNameKey)
	}
	log.Printf("DEBUG: generationConfigMapName=%s", generationConfigMapName)
	broadcastConfigMapName := os.Getenv(broadcastConfigMapNameKey)
	if broadcastConfigMapName == "" {
		log.Fatalf("ERROR: '%s' environment variable must be set", broadcastConfigMapNameKey)
	}
	log.Printf("DEBUG: broadcastConfigMapName=%s", broadcastConfigMapName)
	return podNamespace, jobGlobalIndex, podIndex, generationConfigMapName, broadcastConfigMapName

}

func runBarrier(kubernetesClient *kubernetes.Clientset, namespace string, broadcastConfigMapName string, generationConfigMapName string, workerId string) int {
	log.Printf("INFO: Starting barrier")
	target := getTarget(kubernetesClient, namespace, broadcastConfigMapName)
	log.Printf("DEBUG: target=%d", target)
	generation := target + 1
	log.Printf("DEBUG: generation=%d", generation)
	patchGeneration(kubernetesClient, namespace, generationConfigMapName, workerId, generation)
	log.Printf("DEBUG: Patched")
	waitForBarrierLift(kubernetesClient, namespace, broadcastConfigMapName, generation)
	log.Printf("INFO: Lifted barrier")
	return generation
}

// Get broadcastConfigMap.data.target
func getTarget(kubernetesClient *kubernetes.Clientset, namespace string, broadcastConfigMapName string) int {
	log.Printf("INFO: Getting target")
	broadcastConfigMap, err := kubernetesClient.CoreV1().ConfigMaps(namespace).Get(context.TODO(), broadcastConfigMapName, metav1.GetOptions{})
	if errors.IsNotFound(err) {
		log.Fatalf("ERROR: Broadcast ConfigMap '%s/%s' not found", namespace, broadcastConfigMapName)
	}
	if err != nil {
		log.Fatalf("ERROR: Failed to get broadcast ConfigMap '%s/%s': %v", namespace, broadcastConfigMapName, err)
	}
	rawTarget, ok := broadcastConfigMap.Data[targetKey]
	if !ok {
		log.Fatalf("ERROR: '%s' key not found in broadcast ConfigMap '%s/%s'", targetKey, namespace, broadcastConfigMapName)
	}
	target, err := strconv.Atoi(rawTarget)
	if err != nil {
		log.Fatalf("ERROR: Failed to parse target '%s' from broadcast ConfigMap '%s/%s': %v", rawTarget, namespace, broadcastConfigMapName, err)
	}
	return target
}

// Set generationConfigMap.data[workerId] = generation
func patchGeneration(kubernetesClient *kubernetes.Clientset, namespace string, generationConfigMapName string, workerId string, generation int) {
	log.Printf("INFO: Patching generation")
	patchPayload := map[string]any{
		"data": map[string]string{
			workerId: strconv.Itoa(generation),
		},
	}
	patchBytes, err := json.Marshal(patchPayload)
	if err != nil {
		log.Fatalf("ERROR: Failed to marshal patch payload: %v", err)
	}
	const maxRetries = 5
	var lastErr error
	for i := range maxRetries {
		_, err = kubernetesClient.CoreV1().ConfigMaps(namespace).Patch(context.TODO(), generationConfigMapName, types.StrategicMergePatchType, patchBytes, metav1.PatchOptions{})
		if err == nil {
			return
		}
		lastErr = err
		log.Printf("WARN: Failed to patch generation ConfigMap '%s/%s' on attempt %d/%d: %v", namespace, generationConfigMapName, i+1, maxRetries, err)
		time.Sleep(1 * time.Second)
	}
	log.Fatalf("ERROR: Failed to patch generation ConfigMap '%s/%s' after %d attempts: %v", namespace, generationConfigMapName, maxRetries, lastErr)
}

// Wait until generation <= broadcastConfigMap.data.target
func waitForBarrierLift(kubernetesClient *kubernetes.Clientset, namespace string, broadcastConfigMapName string, generation int) {
	log.Printf("INFO: Waiting for barrier lift")
	for {
		timeout := int64(timeoutSeconds)
		watcher, err := kubernetesClient.CoreV1().ConfigMaps(namespace).Watch(context.TODO(), metav1.ListOptions{
			FieldSelector:  "metadata.name=" + broadcastConfigMapName,
			TimeoutSeconds: &timeout,
		})
		if err != nil {
			log.Fatalf("ERROR: Failed to watch broadcast ConfigMap '%s/%s': %v", namespace, broadcastConfigMapName, err)
		}
		for event := range watcher.ResultChan() {
			if event.Type == watch.Error {
				log.Printf("WARN: Watch for ConfigMap '%s/%s' returned an error, restarting watch: %v", namespace, broadcastConfigMapName, event.Object)
				break
			}
			broadcastConfigMap, ok := event.Object.(*v1.ConfigMap)
			if !ok {
				log.Printf("WARN: Watch event for '%s/%s' was not a ConfigMap, but %T", namespace, broadcastConfigMapName, event.Object)
				continue
			}
			if event.Type == watch.Modified || event.Type == watch.Added {
				rawTarget, ok := broadcastConfigMap.Data[targetKey]
				if !ok {
					continue
				}
				target, err := strconv.Atoi(rawTarget)
				if err != nil {
					log.Printf("WARN: Failed to parse target '%s' from broadcast ConfigMap '%s/%s': %v", rawTarget, namespace, broadcastConfigMapName, err)
					continue
				}
				log.Printf("DEBUG: target=%d", target)
				if generation <= target {
					watcher.Stop()
					return
				}
			}
		}
		log.Printf("WARN: Watch for broadcast ConfigMap '%s/%s' closed, restarting watch", namespace, broadcastConfigMapName)
	}
}

func runAgent(kubernetesClient *kubernetes.Clientset, namespace string, broadcastConfigMapName string, generation int) {
	log.Printf("INFO: Starting agent")
	waitForRestart(kubernetesClient, namespace, broadcastConfigMapName, generation)
}

// Fail program if generation <= broadcastConfigMap.data.restart
func waitForRestart(kubernetesClient *kubernetes.Clientset, namespace string, broadcastConfigMapName string, generation int) {
	log.Printf("INFO: Waiting for restart")
	for {
		timeout := int64(timeoutSeconds)
		watcher, err := kubernetesClient.CoreV1().ConfigMaps(namespace).Watch(context.TODO(), metav1.ListOptions{
			FieldSelector:  "metadata.name=" + broadcastConfigMapName,
			TimeoutSeconds: &timeout,
		})
		if err != nil {
			log.Fatalf("ERROR: Failed to watch broadcast ConfigMap '%s/%s': %v", namespace, broadcastConfigMapName, err)
		}
		for event := range watcher.ResultChan() {
			if event.Type == watch.Error {
				log.Printf("WARN: Watch for ConfigMap '%s/%s' returned an error, restarting watch: %v", namespace, broadcastConfigMapName, event.Object)
				break
			}
			broadcastConfigMap, ok := event.Object.(*v1.ConfigMap)
			if !ok {
				log.Printf("WARN: Watch event for '%s/%s' was not a ConfigMap, but %T", namespace, broadcastConfigMapName, event.Object)
				continue
			}
			if event.Type == watch.Modified || event.Type == watch.Added {
				rawRestart, ok := broadcastConfigMap.Data[restartKey]
				if !ok {
					continue
				}
				restart, err := strconv.Atoi(rawRestart)
				if err != nil {
					log.Printf("WARN: Failed to parse restart '%s' from broadcast ConfigMap '%s/%s': %v", rawRestart, namespace, broadcastConfigMapName, err)
					continue
				}
				log.Printf("DEBUG: restart=%d", restart)
				if generation <= restart {
					log.Printf("INFO: Restart triggered for generation %d (restart target %d). Terminating.", generation, restart)
					os.Exit(42)
				}
			}
		}
		log.Printf("WARN: Watch for broadcast ConfigMap '%s/%s' closed, restarting watch", namespace, broadcastConfigMapName)
	}
}

func runWorker() {
	log.Printf("INFO: Starting worker")
	waitForExitFile()
}

// Exit 0 if /tmp/succeed exists. Exit 1 if /tmp/fail exists
func waitForExitFile() {
	for {
		if _, err := os.Stat("/tmp/succeed"); err == nil {
			log.Printf("INFO: Worker finished successfully, exiting with 0")
			os.Exit(0)
		} else if !os.IsNotExist(err) {
			log.Printf("WARN: Failed to stat /tmp/succeed: %v", err)
		}
		if _, err := os.Stat("/tmp/fail"); err == nil {
			log.Printf("INFO: Worker failed, exiting with 1")
			os.Exit(1)
		} else if !os.IsNotExist(err) {
			log.Printf("WARN: Failed to stat /tmp/fail: %v", err)
		}
		time.Sleep(time.Duration(pollIntervalSeconds) * time.Second)
	}
}
