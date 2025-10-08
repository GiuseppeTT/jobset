package main

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"os"
	"strconv"
	"strings"
	"time"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/apimachinery/pkg/watch"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/rest"
	jobsetv1alpha2 "sigs.k8s.io/jobset/api/jobset/v1alpha2"
	jobSetClient "sigs.k8s.io/jobset/client-go/clientset/versioned"
)

const (
	podNamespaceKey              = "POD_NAMESPACE"
	podNameKey                   = "POD_NAME"
	jobSetNameKey                = "JOBSET_NAME"
	restartPodInPlaceExitCodeKey = "RESTART_POD_IN_PLACE_EXIT_CODE"
	filePollIntervalSeconds      = 1
)

func main() {
	coreClient, jobSetClient, podNamespace, podName, jobSetName, restartPodInPlaceExitCode := setup()
	runAgent(coreClient, jobSetClient, podNamespace, podName, jobSetName, restartPodInPlaceExitCode)
}

func setup() (*kubernetes.Clientset, *jobSetClient.Clientset, string, string, string, int) {
	log.Printf("INFO: Setting up")
	coreClient := getCoreClient()
	jobSetClient := getjobSetClient()
	podNamespace, podName, jobSetName, restartPodInPlaceExitCode := getEnvironmentVariables()
	return coreClient, jobSetClient, podNamespace, podName, jobSetName, restartPodInPlaceExitCode
}

func getCoreClient() *kubernetes.Clientset {
	log.Printf("INFO: Creating core Kubernetes client")
	config, err := rest.InClusterConfig()
	if err != nil {
		log.Fatalf("ERROR: Failed to get in-cluster config: %v", err)
	}
	coreClient, err := kubernetes.NewForConfig(config)
	if err != nil {
		log.Fatalf("ERROR: Failed to create core Kubernetes client: %v", err)
	}
	return coreClient
}

func getjobSetClient() *jobSetClient.Clientset {
	log.Printf("INFO: Creating JobSet Kubernetes client")
	config, err := rest.InClusterConfig()
	if err != nil {
		log.Fatalf("ERROR: Failed to get in-cluster config: %v", err)
	}
	jobSetClient, err := jobSetClient.NewForConfig(config)
	if err != nil {
		log.Fatalf("ERROR: Failed to create JobSe Kubernetes client: %v", err)
	}
	return jobSetClient
}

func getEnvironmentVariables() (string, string, string, int) {
	log.Printf("INFO: Getting environment variables")
	podNamespace := os.Getenv(podNamespaceKey)
	if podNamespace == "" {
		log.Fatalf("ERROR: '%s' environment variable must be set", podNamespaceKey)
	}
	log.Printf("DEBUG: podNamespace=%s", podNamespace)
	podName := os.Getenv(podNameKey)
	if podName == "" {
		log.Fatalf("ERROR: '%s' environment variable must be set", podNameKey)
	}
	log.Printf("DEBUG: podName=%s", podName)
	jobSetName := os.Getenv(jobSetNameKey)
	if jobSetName == "" {
		log.Fatalf("ERROR: '%s' environment variable must be set", jobSetNameKey)
	}
	log.Printf("DEBUG: jobSetName=%s", jobSetName)
	rawRestartPodInPlaceExitCode := os.Getenv(restartPodInPlaceExitCodeKey)
	if rawRestartPodInPlaceExitCode == "" {
		log.Fatalf("ERROR: '%s' environment variable must be set", restartPodInPlaceExitCodeKey)
	}
	restartPodInPlaceExitCode, err := strconv.Atoi(rawRestartPodInPlaceExitCode)
	if err != nil {
		log.Fatalf("ERROR: Failed to parse restartPodInPlaceExitCode: %v", err)
	}
	log.Printf("DEBUG: restartPodInPlaceExitCode=%s", rawRestartPodInPlaceExitCode)
	return podNamespace, podName, jobSetName, restartPodInPlaceExitCode
}

func runAgent(coreClient *kubernetes.Clientset, jobSetClient *jobSetClient.Clientset, podNamespace string, podName string, jobSetName string, restartPodInPlaceExitCode int) {
	log.Printf("INFO: Starting agent")
	watchParentJobSet(coreClient, jobSetClient, podNamespace, podName, jobSetName, restartPodInPlaceExitCode)
}

// Instead of getting the parent JobSet, setting up things and then watching the parent JobSet, do everything only using a watch
// This is done for scalability
// Basically, the agent-workers are expected to start running roughtly at the same time, which creates a thundering herd problem
// By using only a watch instead of a get + watch, we reduce the number of thundering herd problems from two to one
// On top of that, we also add jitter to starting the watch to mitigate the remaining thundering herd problem
// TODO: Add jitter
func watchParentJobSet(coreClient *kubernetes.Clientset, jobSetClient *jobSetClient.Clientset, podNamespace string, podName string, jobSetName string, restartPodInPlaceExitCode int) {
	isBarrierUp := true
	isGenerationPatched := false
	generation := -1
	for {
		log.Printf("INFO: Starting watch for JobSet '%s/%s'", podNamespace, jobSetName)
		watcher, err := jobSetClient.JobsetV1alpha2().JobSets(podNamespace).Watch(context.Background(), metav1.ListOptions{
			FieldSelector: "metadata.name=" + jobSetName,
		})
		if err != nil {
			log.Printf("WARN: Failed to create watch for JobSet: %v. Retrying in 5 seconds", err)
			time.Sleep(5 * time.Second) // TODO: Extract constant
			continue
		}
		processWatchEvents(coreClient, watcher, podNamespace, podName, restartPodInPlaceExitCode, &isBarrierUp, &isGenerationPatched, &generation)
		watcher.Stop()
		log.Printf("WARN: Watch closed. Recreating watch")
	}
}

func processWatchEvents(coreClient *kubernetes.Clientset, watcher watch.Interface, podNamespace string, podName string, restartPodInPlaceExitCode int, isBarrierUp *bool, isGenerationPatched *bool, generation *int) {
	for event := range watcher.ResultChan() {
		log.Printf("INFO: New watch event")
		if !(event.Type == watch.Modified || event.Type == watch.Added) {
			log.Printf("WARN: Event type is not Modified or Added, but %s. Skipping", event.Type)
			continue
		}
		jobSet, ok := event.Object.(*jobsetv1alpha2.JobSet)
		if !ok {
			log.Printf("WARN: Watched object is not a JobSet, but %T. Skipping", event.Object)
			continue
		}
		if !*isGenerationPatched {
			*generation = patchGeneration(coreClient, podNamespace, podName, jobSet)
			*isGenerationPatched = true
		}
		if shouldRestart(jobSet, *generation) {
			log.Printf("INFO: Restarting worker")
			os.Exit(restartPodInPlaceExitCode)
		}
		if *isBarrierUp && shouldLiftBarrier(jobSet, *generation) {
			log.Printf("INFO: Lifting barrier")
			go runWorker()
			*isBarrierUp = false
		}
	}
}

func patchGeneration(coreClient *kubernetes.Clientset, podNamespace string, podName string, jobSet *jobsetv1alpha2.JobSet) int {
	log.Printf("INFO: Patching generation")
	target := int(jobSet.Status.InPlaceRestartTarget)
	generation := target + 1
	log.Printf("DEBUG: generation=%d and target=%d", generation, target)
	escapedInPlaceRestartGenerationKey := strings.ReplaceAll(jobsetv1alpha2.InPlaceRestartGenerationKey, "/", "~1")
	patchPayload := []map[string]any{{
		"op":    "add",
		"path":  fmt.Sprintf("/metadata/annotations/%s", escapedInPlaceRestartGenerationKey),
		"value": strconv.Itoa(generation),
	}}
	patchBytes, err := json.Marshal(patchPayload)
	if err != nil {
		log.Fatalf("ERROR: Failed to marshal patch payload: %v", err)
	}
	for i := range 5 { // TODO: Extract constant maxPatchRetries
		_, err = coreClient.CoreV1().Pods(podNamespace).Patch(context.Background(), podName, types.JSONPatchType, patchBytes, metav1.PatchOptions{})
		if err == nil {
			log.Printf("INFO: Successfully patched pod '%s' with generation '%d'", podName, generation)
			return generation
		}
		log.Printf("WARN: Failed to patch pod '%s' (attempt %d/5): %v. Retrying in %d seconds...", podName, i+1, err, 2*(i+1))
		time.Sleep(time.Duration(2*(i+1)) * time.Second)
	}
	log.Fatalf("ERROR: Failed to patch pod '%s' after 5 attempts: %v", podName, err)
	return -1 // Unreachable
}

func shouldRestart(jobSet *jobsetv1alpha2.JobSet, generation int) bool {
	log.Printf("INFO: Checking if should restart")
	outdated := int(jobSet.Status.InPlaceRestartOutdated)
	log.Printf("DEBUG: generation=%d and outdated=%d", generation, outdated)
	return generation <= outdated
}

func shouldLiftBarrier(jobSet *jobsetv1alpha2.JobSet, generation int) bool {
	log.Printf("INFO: Checking if should lift barrier")
	target := int(jobSet.Status.InPlaceRestartTarget)
	log.Printf("DEBUG: generation=%d and target=%d", generation, target)
	return generation <= target
}

func runWorker() {
	log.Printf("INFO: Starting worker")
	waitForExitFile()
}

// Exit with code <number> if a file named /tmp/exit<number> exists
func waitForExitFile() {
	for {
		entries, err := os.ReadDir("/tmp")
		if err != nil {
			log.Printf("WARN: Failed to read /tmp directory: %v", err)
			time.Sleep(time.Duration(filePollIntervalSeconds) * time.Second)
			continue
		}
		for _, entry := range entries {
			if !entry.IsDir() && strings.HasPrefix(entry.Name(), "exit") {
				exitCodeStr := strings.TrimPrefix(entry.Name(), "exit")
				if exitCode, err := strconv.Atoi(exitCodeStr); err == nil {
					log.Printf("INFO: Found exit file %s. Exiting with code %d", entry.Name(), exitCode)
					os.Exit(exitCode)
				}
			}
		}
		time.Sleep(time.Duration(filePollIntervalSeconds) * time.Second)
	}
}
