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
	jobSetClientSet "sigs.k8s.io/jobset/client-go/clientset/versioned"
)

const (
	// Environment variables
	namespaceKey                 = "NAMESPACE"
	podNameKey                   = "POD_NAME"
	jobSetNameKey                = "JOBSET_NAME"
	restartPodInPlaceExitCodeKey = "RESTART_POD_IN_PLACE_EXIT_CODE"
	workerCommandKey             = "WORKER_COMMAND"
	initialBackOffKey            = "INITIAL_BACKOFF"
	maxBackOffKey                = "MAX_BACKOFF"
	jitterBackOffFactorKey       = "JITTER_BACKOFF_FACTOR"
)

func main() {
	log.Printf("INFO: Starting agent program")
	coreClient := getCoreClient()
	jobSetClient := getjobSetClient()
	agentConfig := getAgentConfig()
	agent := NewAgent(coreClient, jobSetClient, agentConfig)
	agent.Run()
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

func getjobSetClient() *jobSetClientSet.Clientset {
	log.Printf("INFO: Creating JobSet Kubernetes client")
	config, err := rest.InClusterConfig()
	if err != nil {
		log.Fatalf("ERROR: Failed to get in-cluster config: %v", err)
	}
	jobSetClient, err := jobSetClientSet.NewForConfig(config)
	if err != nil {
		log.Fatalf("ERROR: Failed to create JobSe Kubernetes client: %v", err)
	}
	return jobSetClient
}

type AgentConfig struct {
	Namespace                 string
	PodName                   string
	JobSetName                string
	RestartPodInPlaceExitCode int
	WorkerCommand             string
	InitialBackOff            time.Duration
	MaxBackOff                time.Duration
	JitterBackOffFactor       float64
}

func NewAgentConfig(
	namespace string,
	podName string,
	jobSetName string,
	restartPodInPlaceExitCode int,
	workerCommand string,
	initialBackOff time.Duration,
	maxBackOff time.Duration,
	jitterBackOffFactor float64,
) AgentConfig {
	return AgentConfig{
		Namespace:                 namespace,
		PodName:                   podName,
		JobSetName:                jobSetName,
		RestartPodInPlaceExitCode: restartPodInPlaceExitCode,
		WorkerCommand:             workerCommand,
		InitialBackOff:            initialBackOff,
		MaxBackOff:                maxBackOff,
		JitterBackOffFactor:       jitterBackOffFactor,
	}
}

func getAgentConfig() AgentConfig {
	log.Printf("INFO: Getting agent config")
	namespace := getRequiredStringEnv(namespaceKey)
	podName := getRequiredStringEnv(podNameKey)
	jobSetName := getRequiredStringEnv(jobSetNameKey)
	restartPodInPlaceExitCode := getRequiredIntEnv(restartPodInPlaceExitCodeKey)
	workerCommand := getRequiredStringEnv(workerCommandKey)
	initialBackOff := getRequiredDurationEnv(initialBackOffKey)
	maxBackOff := getRequiredDurationEnv(maxBackOffKey)
	jitterBackOffFactor := getRequiredFloat64Env(jitterBackOffFactorKey)
	return NewAgentConfig(
		namespace,
		podName,
		jobSetName,
		restartPodInPlaceExitCode,
		workerCommand,
		initialBackOff,
		maxBackOff,
		jitterBackOffFactor,
	)
}

func getRequiredStringEnv(key string) string {
	value := os.Getenv(key)
	if value == "" {
		log.Fatalf("ERROR: '%s' environment variable must be set", key)
	}
	log.Printf("DEBUG: %s=%s", key, value)
	return value
}

func getRequiredIntEnv(key string) int {
	valueStr := getRequiredStringEnv(key)
	value, err := strconv.Atoi(valueStr)
	if err != nil {
		log.Fatalf("ERROR: Failed to parse %s: %v", key, err)
	}
	log.Printf("DEBUG: %s=%d", key, value)
	return value
}

func getRequiredFloat64Env(key string) float64 {
	valueStr := getRequiredStringEnv(key)
	value, err := strconv.ParseFloat(valueStr, 64)
	if err != nil {
		log.Fatalf("ERROR: Failed to parse %s: %v", key, err)
	}
	log.Printf("DEBUG: %s=%f", key, value)
	return value
}

func getRequiredDurationEnv(key string) time.Duration {
	valueStr := getRequiredStringEnv(key)
	value, err := time.ParseDuration(valueStr)
	if err != nil {
		log.Fatalf("ERROR: Failed to parse %s: %v", key, err)
	}
	log.Printf("DEBUG: %s=%s", key, value)
	return value
}

type Agent struct {
	coreClient     *kubernetes.Clientset
	jobSetClient   *jobSetClientSet.Clientset
	config         AgentConfig
	epoch          int
	isEpochPatched bool
	isBarrierUp    bool
}

func NewAgent(
	coreClient *kubernetes.Clientset,
	jobSetClient *jobSetClientSet.Clientset,
	config AgentConfig,
) *Agent {
	return &Agent{
		coreClient:     coreClient,
		jobSetClient:   jobSetClient,
		config:         config,
		epoch:          -1,
		isEpochPatched: false,
		isBarrierUp:    true,
	}
}

func (a *Agent) Run() {
	log.Printf("INFO: Starting agent")
	a.watchParentJobSet()
}

// Instead of getting the parent JobSet, setting up things and then watching the parent JobSet, do everything only using a watch
// This is done for scalability
// Basically, the agents are expected to start running roughtly at the same time, which creates a thundering herd problem
// By using only a watch instead of a get + watch, we reduce the number of thundering herd problems from two to one
// On top of that, we also add jitter to starting the watch to mitigate the remaining thundering herd problem
func (a *Agent) watchParentJobSet() {
	log.Printf("INFO: Watching parent JobSet '%s/%s'", a.config.Namespace, a.config.JobSetName)
	for {
		ctx := context.Background()
		watcher, err := a.createWatcherWithJitter(ctx)
		if err != nil {
			log.Printf("WARN: Failed to create watcher for parent JobSet '%s/%s'. Retrying in 5 seconds: %v", a.config.Namespace, a.config.JobSetName, err)
			time.Sleep(5 * time.Second) // TODO: Extract constant
			continue
		}
		a.processWatchEvents(watcher)
		watcher.Stop()
		log.Printf("WARN: Watch closed. Recreating watch")
	}
}

// Create a watcher with jitter on **watcher creation**
// Once the watcher is created, there is no jitter to continiue watching
// Based on k8s.io/client-go@v0.34.1/gentype/type.go func (c *Client[T]) Watch
func (a *Agent) createWatcherWithJitter(ctx context.Context) (watch.Interface, error) {
	log.Printf("INFO: Creating watcher on parent JobSet '%s/%s'", a.config.Namespace, a.config.JobSetName)
	opts := metav1.ListOptions{
		FieldSelector: "metadata.name=" + a.config.JobSetName,
		Watch:         true,
	}
	timeout := 10 * time.Minute // TODO: Extract constant
	backOffManager := &rest.URLBackoff{
		Backoff: flowcontrol.NewBackOffWithJitter(a.config.InitialBackOff, a.config.MaxBackOff, a.config.JitterBackOffFactor),
	}
	request := a.jobSetClient.
		JobsetV1alpha2().
		RESTClient().
		Get().
		Namespace(a.config.Namespace).
		Resource("jobsets"). // TODO: Extract constant or get from JobSet library
		VersionedParams(&opts, metav1.ParameterCodec).
		Timeout(timeout).
		BackOffWithContext(backOffManager)

	return request.Watch(ctx)
}

func (a *Agent) processWatchEvents(watcher watch.Interface) {
	log.Printf("INFO: Processing watch events from parent JobSet '%s/%s'", a.config.Namespace, a.config.JobSetName)
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
		if a.shouldPatchEpoch() {
			a.patchEpoch(jobSet)
		}
		if a.shouldRestartInPlace(jobSet) {
			a.restartInPlace()
		}
		if a.shouldLiftBarrier(jobSet) {
			a.liftBarrier()
		}
	}
}

func (a *Agent) shouldPatchEpoch() bool {
	return !a.isEpochPatched
}

func (a *Agent) patchEpoch(jobSet *jobsetv1alpha2.JobSet) {
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
	for i := range 5 { // TODO: Extract constant
		_, err = a.coreClient.CoreV1().Pods(a.config.Namespace).Patch(context.Background(), a.config.PodName, types.JSONPatchType, patchBytes, metav1.PatchOptions{})
		if err == nil {
			log.Printf("INFO: Successfully patched Pod '%s/%s' with epoch '%d'", a.config.Namespace, a.config.PodName, epoch)
			break
		}
		if i == 4 {
			log.Fatalf("ERROR: Failed to patch pod '%s/%s' after 4 attempts: %v", a.config.Namespace, a.config.PodName, err)
		}
		sleepDuration := time.Duration(2*(i+1)) * time.Second
		log.Printf("WARN: Failed to patch Pod '%s/%s' (attempt %d/4): Retrying in %v seconds: %v", a.config.Namespace, a.config.PodName, i+1, sleepDuration, err)
		time.Sleep(sleepDuration)
	}
	a.epoch = epoch
	a.isEpochPatched = true
}

func (a *Agent) shouldRestartInPlace(jobSet *jobsetv1alpha2.JobSet) bool {
	log.Printf("INFO: Checking if should restart")
	mostRecentDeprecatedEpoch := int(jobSet.Status.DeprecatedEpoch)
	log.Printf("DEBUG: epoch=%d and deprecatedEpoch=%d", a.epoch, mostRecentDeprecatedEpoch)
	return a.epoch <= mostRecentDeprecatedEpoch
}

func (a *Agent) restartInPlace() {
	log.Printf("INFO: Restarting Pod in place")
	os.Exit(a.config.RestartPodInPlaceExitCode)
}

func (a *Agent) shouldLiftBarrier(jobSet *jobsetv1alpha2.JobSet) bool {
	log.Printf("INFO: Checking if should lift barrier")
	mostRecentSyncedEpoch := int(jobSet.Status.SyncedEpoch)
	log.Printf("DEBUG: epoch=%d and syncedEpoch=%d", a.epoch, mostRecentSyncedEpoch)
	return a.isBarrierUp && a.epoch == mostRecentSyncedEpoch
}

func (a *Agent) liftBarrier() {
	log.Printf("INFO: Lifting barrier")
	go a.runWorker()
	a.isBarrierUp = false
}

func (a *Agent) runWorker() {
	log.Printf("INFO: Starting worker")
	cmd := exec.Command("bash", "-c", a.config.WorkerCommand)
	cmd.Stdout = os.Stdout
	cmd.Stderr = os.Stderr
	if err := cmd.Run(); err != nil {
		if exitError, ok := err.(*exec.ExitError); ok {
			os.Exit(exitError.ExitCode())
		}
		log.Fatalf("ERROR: worker command failed with non-exit error: %v", err)
	}
	os.Exit(0)
}
