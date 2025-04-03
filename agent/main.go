package main

import (
	"agent/agent"
	"context"
	"flag"
	"fmt"
	"os"

	"k8s.io/klog/v2"
)

var (
	workerJobsetUid           = flag.String("worker-jobset-uid", "", "JobSet UID of the worker")
	workerId                  = flag.String("worker-id", "", "ID of the worker")
	workerPodUid              = flag.String("worker-pod-uid", "", "Pod UID of the worker")
	workerContainerName       = flag.String("worker-container-name", "", "Container name of the worker")
	workerTerminationPeriod   = flag.Duration("worker-termination-grace-period", -1, "Termination grace period of the worker when it must be killed")
	criSocketPath             = flag.String("cri-socket-path", "", "Path of the CRI socket")
	criPollingInterval        = flag.Duration("cri-polling-interval", -1, "Polling interval for the CRI API")
	redisAddress              = flag.String("redis-address", "", "Address of the Redis service")
	redisBroadcastChannelName = flag.String("redis-broadcast-channel-name", "", "Name of the broadcast channel")
)

func main() {
	klog.InitFlags(nil)
	defer klog.Flush()
	err := parseFlags()
	if err != nil {
		klog.Errorf("Failed to parse flags: %v", err)
		os.Exit(1)
	}

	agent := agent.NewAgent(
		*workerJobsetUid,
		*workerId,
		*workerPodUid,
		*workerContainerName,
		*workerTerminationPeriod,
		*criSocketPath,
		*criPollingInterval,
		*redisAddress,
		*redisBroadcastChannelName,
	)
	ctx := context.Background()
	err = agent.Initialize(ctx)
	if err != nil {
		klog.Errorf("Failed to initialize agent: %v", err)
		os.Exit(1)
	}
	defer agent.Close()

	agent.Run(ctx)
}

func parseFlags() error {
	klog.Infof("Parsing flags")
	flag.Parse()
	if *workerJobsetUid == "" {
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
	if *workerTerminationPeriod < 0 {
		return fmt.Errorf("argument '--worker-termination-grace-period' is required")
	}
	if *criSocketPath == "" {
		return fmt.Errorf("argument '--cri-socket-path' is required")
	}
	if *criPollingInterval < 0 {
		return fmt.Errorf("argument '--cri-polling-interval' is required")
	}
	if *redisAddress == "" {
		return fmt.Errorf("argument '--redis-address' is required")
	}
	if *redisBroadcastChannelName == "" {
		return fmt.Errorf("argument '--redis-broadcast-channel-name' is required")
	}
	return nil
}
