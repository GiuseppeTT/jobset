package main

import (
	"context"
	"encoding/json"
	"log"
	"math/rand"
	"os"
	"strconv"
	"time"

	v1 "k8s.io/api/core/v1"
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
	watchTimeoutSeconds        = 1200
	filePollIntervalSeconds    = 1
)

func main() {
	kubernetesClient := getKubernetesClient()
	podNamespace, jobGlobalIndex, podIndex, generationConfigMapName, broadcastConfigMapName := getEnvironmentVariables()
	workerId := jobGlobalIndex + "-" + podIndex
	run(kubernetesClient, podNamespace, broadcastConfigMapName, generationConfigMapName, workerId)
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

func run(kubernetesClient *kubernetes.Clientset, namespace string, broadcastConfigMapName string, generationConfigMapName string, workerId string) {
	log.Printf("INFO: Running")
	rand.Seed(time.Now().UnixNano())
	isBarrierUp := true
	isGenerationPatched := false
	generation := -1
	for {
		// Spread initial load over 15 seconds
		initialDelay := time.Duration(rand.Intn(10*1000)) * time.Millisecond // Distribute load over 10 seconds
		time.Sleep(initialDelay)
		timeout := int64(watchTimeoutSeconds)
		log.Printf("INFO: Running after %v delay", initialDelay)
		log.Printf("INFO: Creating watch for broadcast ConfigMap '%s/%s'", namespace, broadcastConfigMapName)
		watcher, err := kubernetesClient.CoreV1().ConfigMaps(namespace).Watch(context.TODO(), metav1.ListOptions{
			FieldSelector:  "metadata.name=" + broadcastConfigMapName,
			TimeoutSeconds: &timeout,
		})
		log.Printf("DEBUG: Created broadcast ConfigMap '%s/%s'", namespace, broadcastConfigMapName)
		if err != nil {
			log.Fatalf("ERROR: Failed to watch broadcast ConfigMap '%s/%s': %v", namespace, broadcastConfigMapName, err)
		}
		for event := range watcher.ResultChan() {
			log.Printf("INFO: New watch event")
			if event.Type == watch.Error {
				log.Printf("WARN: Watch for ConfigMap '%s/%s' returned an error, restarting watch: %v", namespace, broadcastConfigMapName, event.Object)
				break
			}
			broadcastConfigMap, ok := event.Object.(*v1.ConfigMap)
			if !ok {
				log.Printf("WARN: Watch event for '%s/%s' is not a ConfigMap, but %T", namespace, broadcastConfigMapName, event.Object)
				continue
			}
			if event.Type == watch.Modified || event.Type == watch.Added {
				log.Printf("INFO: New added / modified watch event")
				if !isGenerationPatched {
					generation = patchGeneration(kubernetesClient, broadcastConfigMap, namespace, generationConfigMapName, workerId)
					isGenerationPatched = true
				}
				if mustRestart(broadcastConfigMap, generation) {
					log.Printf("INFO: Restarting worker")
					os.Exit(42)
				}
				if isBarrierUp && shouldLiftBarrier(broadcastConfigMap, generation) {
					log.Printf("INFO: Lifting barrier")
					go runWorker()
					isBarrierUp = false
				}
			}
		}
		log.Printf("WARN: Watch for broadcast ConfigMap '%s/%s' closed, restarting watch", namespace, broadcastConfigMapName)
	}
}

// Set generationConfigMap.data[workerId] = generation
func patchGeneration(kubernetesClient *kubernetes.Clientset, broadcastConfigMap *v1.ConfigMap, namespace string, generationConfigMapName string, workerId string) int {
	log.Printf("INFO: Patching generation")
	rawTarget, ok := broadcastConfigMap.Data[targetKey]
	if !ok {
		log.Fatalf("ERROR: '%s' key not found in broadcast ConfigMap '%s/%s'", targetKey, broadcastConfigMap.Namespace, broadcastConfigMap.Name)
	}
	target, err := strconv.Atoi(rawTarget)
	if err != nil {
		log.Fatalf("ERROR: Failed to parse target '%s' from broadcast ConfigMap '%s/%s': %v", rawTarget, broadcastConfigMap.Namespace, broadcastConfigMap.Name, err)
	}
	generation := target + 1
	log.Printf("DEBUG: target=%d and generation=%d", target, generation)
	patchPayload := v1.ConfigMap{
		TypeMeta: metav1.TypeMeta{
			APIVersion: "v1",
			Kind:       "ConfigMap",
		},
		ObjectMeta: metav1.ObjectMeta{
			Name:      generationConfigMapName + "-" + workerId,
			Namespace: namespace,
			//OwnerReferences: []metav1.OwnerReference{}, // TODO: Add Pod as owner
			Labels: map[string]string{
				"jobset.sigs.k8s.io/jobset-name":              "jobset-benchmark",        // TODO: Get from env variable
				"alpha.jobset.sigs.k8s.io/restart-group-name": "restart-group-benchmark", // TODO: Get from env variable
			},
		},
		Data: map[string]string{
			"generation": strconv.Itoa(generation),
		},
	}
	patchBytes, err := json.Marshal(patchPayload)
	if err != nil {
		log.Fatalf("ERROR: Failed to marshal patch payload: %v", err)
	}
	const maxRetries = 5
	var lastErr error
	for i := range maxRetries {
		_, err = kubernetesClient.CoreV1().ConfigMaps(namespace).Patch(context.TODO(), generationConfigMapName+"-"+workerId, types.ApplyPatchType, patchBytes, metav1.PatchOptions{
			FieldManager: "test",
		})
		if err == nil {
			log.Printf("DEBUG: Patched generation=%d", generation)
			return generation
		}
		lastErr = err
		log.Printf("WARN: Failed to patch generation ConfigMap '%s/%s' on attempt %d/%d: %v", namespace, generationConfigMapName+"-"+workerId, i+1, maxRetries, err)
		time.Sleep(1 * time.Second)
	}
	log.Fatalf("ERROR: Failed to patch generation ConfigMap '%s/%s' after %d attempts: %v", namespace, generationConfigMapName+"-"+workerId, maxRetries, lastErr)
	return -1
}

func mustRestart(broadcastConfigMap *v1.ConfigMap, generation int) bool {
	log.Printf("INFO: Checking if must restart")
	rawRestart, ok := broadcastConfigMap.Data[restartKey]
	if !ok {
		log.Fatalf("ERROR: '%s' key not found in broadcast ConfigMap '%s/%s'", restartKey, broadcastConfigMap.Namespace, broadcastConfigMap.Name)
	}
	restart, err := strconv.Atoi(rawRestart)
	if err != nil {
		log.Fatalf("ERROR: Failed to parse restart '%s' from broadcast ConfigMap '%s/%s': %v", rawRestart, broadcastConfigMap.Namespace, broadcastConfigMap.Name, err)
	}
	log.Printf("DEBUG: restart=%d and generation=%d", restart, generation)
	return generation <= restart
}

func shouldLiftBarrier(broadcastConfigMap *v1.ConfigMap, generation int) bool {
	log.Printf("INFO: Checking if should lift barrier")
	rawTarget, ok := broadcastConfigMap.Data[targetKey]
	if !ok {
		log.Fatalf("ERROR: '%s' key not found in broadcast ConfigMap '%s/%s'", targetKey, broadcastConfigMap.Namespace, broadcastConfigMap.Name)
	}
	target, err := strconv.Atoi(rawTarget)
	if err != nil {
		log.Fatalf("ERROR: Failed to parse target '%s' from broadcast ConfigMap '%s/%s': %v", rawTarget, broadcastConfigMap.Namespace, broadcastConfigMap.Name, err)
	}
	log.Printf("DEBUG: target=%d and generation=%d", target, generation)
	return generation <= target
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
		time.Sleep(time.Duration(filePollIntervalSeconds) * time.Second)
	}
}
