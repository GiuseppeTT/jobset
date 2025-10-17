package main

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"os"
	"os/exec"
	"strconv"
	"strings"
	"time"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/apimachinery/pkg/watch"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/rest"
	"k8s.io/client-go/util/flowcontrol"
	jobsetv1alpha2 "sigs.k8s.io/jobset/api/jobset/v1alpha2"
	jobSetClient "sigs.k8s.io/jobset/client-go/clientset/versioned" // TODO: Find a better name
)

const (
	namespaceKey                 = "NAMESPACE"
	podNameKey                   = "POD_NAME"
	jobSetNameKey                = "JOBSET_NAME"
	restartPodInPlaceExitCodeKey = "RESTART_POD_IN_PLACE_EXIT_CODE"
	workerCommandKey             = "WORKER_COMMAND"
	initialBackOffKey            = "INITIAL_BACKOFF"
	maxBackOffKey                = "MAX_BACKOFF"
	jitterBackOffFactorKey       = "JITTER_BACKOFF_FACTOR"
	filePollIntervalSeconds      = 1
)

func main() {
	coreClient, jobSetClient, namespace, podName, jobSetName, restartPodInPlaceExitCode, workerCommand, maxBackOff, initialBackOff, jitterBackOffFactor := setup()
	runAgent(coreClient, jobSetClient, namespace, podName, jobSetName, restartPodInPlaceExitCode, workerCommand, maxBackOff, initialBackOff, jitterBackOffFactor)
}

func setup() (*kubernetes.Clientset, *jobSetClient.Clientset, string, string, string, int, string, time.Duration, time.Duration, float64) {
	log.Printf("INFO: Setting up")
	coreClient := getCoreClient()
	jobSetClient := getjobSetClient()
	namespace, podName, jobSetName, restartPodInPlaceExitCode, workerCommand, maxBackOff, initialBackOff, jitterBackOffFactor := getEnvironmentVariables()
	return coreClient, jobSetClient, namespace, podName, jobSetName, restartPodInPlaceExitCode, workerCommand, maxBackOff, initialBackOff, jitterBackOffFactor
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

func getEnvironmentVariables() (string, string, string, int, string, time.Duration, time.Duration, float64) {
	log.Printf("INFO: Getting environment variables")
	namespace := os.Getenv(namespaceKey)
	if namespace == "" {
		log.Fatalf("ERROR: '%s' environment variable must be set", namespaceKey)
	}
	log.Printf("DEBUG: namespace=%s", namespace)
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
	workerCommand := os.Getenv(workerCommandKey)
	if workerCommand == "" {
		log.Fatalf("ERROR: '%s' environment variable must be set", workerCommandKey)
	}
	log.Printf("DEBUG: workerCommand=%s", workerCommand)
	initialBackOffRaw := os.Getenv(initialBackOffKey)
	if initialBackOffRaw == "" {
		log.Fatalf("ERROR: '%s' environment variable must be set", initialBackOffKey)
	}
	initialBackOff, err := time.ParseDuration(initialBackOffRaw)
	if err != nil {
		log.Fatalf("ERROR: Failed to parse initialBackOff: %v", err)
	}
	log.Printf("DEBUG: initialBackOff=%v", initialBackOff)
	maxBackOffRaw := os.Getenv(maxBackOffKey)
	if maxBackOffRaw == "" {
		log.Fatalf("ERROR: '%s' environment variable must be set", maxBackOffKey)
	}
	maxBackOff, err := time.ParseDuration(maxBackOffRaw)
	if err != nil {
		log.Fatalf("ERROR: Failed to parse maxBackOff: %v", err)
	}
	log.Printf("DEBUG: maxBackOff=%v", maxBackOff)
	// Parse
	jitterBackOffFactorRaw := os.Getenv(jitterBackOffFactorKey)
	if jitterBackOffFactorRaw == "" {
		log.Fatalf("ERROR: '%s' environment variable must be set", jitterBackOffFactorKey)
	}
	jitterBackOffFactor, err := strconv.ParseFloat(jitterBackOffFactorRaw, 64)
	if err != nil {
		log.Fatalf("ERROR: Failed to parse jitterBackOffFactor: %v", err)
	}
	log.Printf("DEBUG: jitterBackOffFactor=%v", jitterBackOffFactor)
	return namespace, podName, jobSetName, restartPodInPlaceExitCode, workerCommand, initialBackOff, maxBackOff, jitterBackOffFactor
}

func runAgent(
	coreClient *kubernetes.Clientset,
	jobSetClient *jobSetClient.Clientset,
	namespace string,
	podName string,
	jobSetName string,
	restartPodInPlaceExitCode int,
	workerCommand string,
	maxBackOff time.Duration,
	initialBackOff time.Duration,
	jitterBackOffFactor float64,
) {
	log.Printf("INFO: Starting agent")
	watchParentJobSet(coreClient, jobSetClient, namespace, podName, jobSetName, restartPodInPlaceExitCode, workerCommand, maxBackOff, initialBackOff, jitterBackOffFactor)
}

// Instead of getting the parent JobSet, setting up things and then watching the parent JobSet, do everything only using a watch
// This is done for scalability
// Basically, the agents are expected to start running roughtly at the same time, which creates a thundering herd problem
// By using only a watch instead of a get + watch, we reduce the number of thundering herd problems from two to one
// On top of that, we also add jitter to starting the watch to mitigate the remaining thundering herd problem
func watchParentJobSet(
	coreClient *kubernetes.Clientset,
	jobSetClient *jobSetClient.Clientset,
	namespace string,
	podName string,
	jobSetName string,
	restartPodInPlaceExitCode int,
	workerCommand string,
	maxBackOff time.Duration,
	initialBackOff time.Duration,
	jitterBackOffFactor float64,
) {
	log.Printf("INFO: Watching parent JobSet")
	isBarrierUp := true
	isEpochPatched := false
	epoch := -1
	for {
		watcher, err := WatchWithJitter(context.Background(), jobSetClient, namespace, jobSetName, maxBackOff, initialBackOff, jitterBackOffFactor)
		if err != nil {
			log.Printf("WARN: Failed to create watch for JobSet: %v. Retrying in 5 seconds", err)
			time.Sleep(5 * time.Second) // TODO: Extract constant
			continue
		}
		processWatchEvents(coreClient, watcher, namespace, podName, restartPodInPlaceExitCode, &isBarrierUp, &isEpochPatched, &epoch, workerCommand)
		watcher.Stop()
		log.Printf("WARN: Watch closed. Recreating watch")
	}
}

// Based on k8s.io/client-go@v0.34.1/gentype/type.go func (c *Client[T]) Watch
func WatchWithJitter(ctx context.Context, jobSetClient *jobSetClient.Clientset, namespace, jobSetName string, maxBackOff, initialBackOff time.Duration, jitterBackOffFactor float64) (watch.Interface, error) {
	log.Printf("INFO: Watching JobSet '%s/%s'", namespace, jobSetName)
	opts := metav1.ListOptions{
		FieldSelector: "metadata.name=" + jobSetName,
		Watch:         true,
	}
	timeout := 10 * time.Minute // TODO: Extract constant
	backOffManager := &rest.URLBackoff{
		Backoff: flowcontrol.NewBackOffWithJitter(initialBackOff, maxBackOff, jitterBackOffFactor),
	}
	request := jobSetClient.
		JobsetV1alpha2().
		RESTClient().
		Get().
		Namespace(namespace).
		Resource("jobsets").
		VersionedParams(&opts, metav1.ParameterCodec).
		Timeout(timeout).
		BackOffWithContext(backOffManager)

	return request.Watch(ctx)
}

func processWatchEvents(coreClient *kubernetes.Clientset, watcher watch.Interface, namespace string, podName string, restartPodInPlaceExitCode int, isBarrierUp *bool, isEpochPatched *bool, epoch *int, workerCommand string) {
	log.Printf("INFO: Processing watch events")
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
		if !*isEpochPatched {
			*epoch = patchEpoch(coreClient, namespace, podName, jobSet)
			*isEpochPatched = true
		}
		if shouldRestart(jobSet, *epoch) {
			log.Printf("INFO: Restarting Pod in place")
			os.Exit(restartPodInPlaceExitCode)
		}
		if *isBarrierUp && shouldLiftBarrier(jobSet, *epoch) {
			log.Printf("INFO: Lifting barrier")
			go runWorker(workerCommand)
			*isBarrierUp = false
		}
	}
}

func patchEpoch(coreClient *kubernetes.Clientset, namespace string, podName string, jobSet *jobsetv1alpha2.JobSet) int {
	log.Printf("INFO: Patching epoch")
	mostRecentSyncedEpoch := int(jobSet.Status.SyncedEpoch)
	epoch := mostRecentSyncedEpoch + 1
	log.Printf("DEBUG: epoch=%d and syncedEpoch=%d", epoch, mostRecentSyncedEpoch)
	escapedEpochKey := strings.ReplaceAll(jobsetv1alpha2.EpochKey, "/", "~1")
	patchPayload := []map[string]any{{
		"op":    "add",
		"path":  fmt.Sprintf("/metadata/annotations/%s", escapedEpochKey),
		"value": strconv.Itoa(epoch),
	}}
	patchBytes, err := json.Marshal(patchPayload)
	if err != nil {
		log.Fatalf("ERROR: Failed to marshal patch payload: %v", err)
	}
	for i := range 5 { // TODO: Extract constant maxPatchRetries
		_, err = coreClient.CoreV1().Pods(namespace).Patch(context.Background(), podName, types.JSONPatchType, patchBytes, metav1.PatchOptions{})
		if err == nil {
			log.Printf("INFO: Successfully patched pod '%s' with epoch '%d'", podName, epoch)
			return epoch
		}
		log.Printf("WARN: Failed to patch pod '%s' (attempt %d/5): %v. Retrying in %d seconds...", podName, i+1, err, 2*(i+1))
		time.Sleep(time.Duration(2*(i+1)) * time.Second)
	}
	log.Fatalf("ERROR: Failed to patch pod '%s' after 5 attempts: %v", podName, err)
	return -1 // Unreachable
}

func shouldRestart(jobSet *jobsetv1alpha2.JobSet, epoch int) bool {
	log.Printf("INFO: Checking if should restart")
	mostRecentDeprecatedEpoch := int(jobSet.Status.DeprecatedEpoch)
	log.Printf("DEBUG: epoch=%d and deprecatedEpoch=%d", epoch, mostRecentDeprecatedEpoch)
	return epoch <= mostRecentDeprecatedEpoch
}

func shouldLiftBarrier(jobSet *jobsetv1alpha2.JobSet, epoch int) bool {
	log.Printf("INFO: Checking if should lift barrier")
	mostRecentSyncedEpoch := int(jobSet.Status.SyncedEpoch)
	log.Printf("DEBUG: epoch=%d and target=%d", epoch, mostRecentSyncedEpoch)
	return epoch == mostRecentSyncedEpoch
}

func runWorker(workerCommand string) {
	log.Printf("INFO: Starting worker")
	cmd := exec.Command("bash", "-c", workerCommand)
	cmd.Stdout = os.Stdout
	cmd.Stderr = os.Stderr
	if err := cmd.Run(); err != nil {
		if exitError, ok := err.(*exec.ExitError); ok {
			os.Exit(exitError.ExitCode())
		}
		log.Fatalf("ERROR: workerCommand failed with non-exit error: %v", err)
	}
	os.Exit(0)
}
