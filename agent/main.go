package main

import (
	"agent/agent"
	"context"
	"flag"
	"fmt"
	"net/http"
	"os"

	"k8s.io/klog/v2"
)

const (
	readyProbePath = "/ready"
	readyProbePort = 8080
)

var (
	workerJobSetUid              = flag.String("worker-jobset-uid", "", "JobSet UID of the worker")
	workerId                     = flag.String("worker-id", "", "ID of the worker. Must be unique in the JobSet")
	workerPodUid                 = flag.String("worker-pod-uid", "", "Pod UID of the worker. The agent must run in the same pod as a sidecar")
	workerContainerName          = flag.String("worker-container-name", "", "Container name of the worker")
	workerTerminationGracePeriod = flag.Duration("worker-termination-grace-period", -1, "Termination grace period of the worker when it must be killed. The flag accepts a value acceptable to time.ParseDuration")
	criSocketPath                = flag.String("cri-socket-path", "", "Path of the CRI socket")
	criPollingInterval           = flag.Duration("cri-polling-interval", -1, "Polling interval for the CRI API. The flag accepts a value acceptable to time.ParseDuration")
	valkeyAddress                = flag.String("valkey-address", "", "Address of the Valkey service")
)

func main() {
	klog.InitFlags(nil)
	defer klog.Flush()
	flag.Parse()
	err := validateFlags()
	if err != nil {
		klog.Errorf("Failed to validate flags: %v", err)
		os.Exit(1)
	}
	agent, err := agent.NewAgent(
		*workerJobSetUid,
		*workerId,
		*workerPodUid,
		*workerContainerName,
		*workerTerminationGracePeriod,
		*criSocketPath,
		*criPollingInterval,
		*valkeyAddress,
	)
	if err != nil {
		klog.Errorf("Failed to create agent: %v", err)
		os.Exit(1)
	}
	defer agent.Close()
	err = agent.HandleInitializationCases()
	if err != nil {
		klog.Errorf("Failed to handle initialization cases: %v", err)
		os.Exit(1)
	}
	go serveReadyProbe()
	ctx := context.Background()
	agent.Run(ctx)
}

func validateFlags() error {
	klog.Info("Validating flags")
	if *workerJobSetUid == "" {
		return fmt.Errorf("argument '--worker-jobset-uid' is required")
	}
	if *workerId == "" {
		return fmt.Errorf("argument '--worker-id' is required")
	}
	if *workerPodUid == "" {
		return fmt.Errorf("argument '--worker-pod-uid' is required")
	}
	if *workerContainerName == "" {
		return fmt.Errorf("argument '--worker-container-name' is required")
	}
	if *workerTerminationGracePeriod < 0 {
		return fmt.Errorf("argument '--worker-termination-grace-period' is required")
	}
	if *criSocketPath == "" {
		return fmt.Errorf("argument '--cri-socket-path' is required")
	}
	if *criPollingInterval < 0 {
		return fmt.Errorf("argument '--cri-polling-interval' is required")
	}
	if *valkeyAddress == "" {
		return fmt.Errorf("argument '--valkey-address' is required")
	}
	return nil
}

func serveReadyProbe() {
	klog.Info("Serving ready probe")
	http.HandleFunc(readyProbePath, func(w http.ResponseWriter, r *http.Request) {})
	address := fmt.Sprintf(":%d", readyProbePort)
	err := http.ListenAndServe(address, nil)
	if err != nil {
		klog.Errorf("Failed to serve ready probe: %v", err)
	}
}
