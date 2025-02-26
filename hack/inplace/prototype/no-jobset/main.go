package main

import (
	"context"
	"flag"
	"inplace/agent"
	"time"

	"k8s.io/klog/v2"
)

var (
	// Target container
	targetPodUid        = flag.String("target-pod-uid", "", "Pod UID of the the target container. Should be the same pod in which this agent is running")
	targetContainerName = flag.String("target-container-name", "", "Container name of the target container")
	// CRI
	criSocketPath      = flag.String("cri-socket-path", "unix:///run/containerd/containerd.sock", "Path of the CRI socket")
	criPollingInterval = flag.Duration("cri-polling-interval", 1*time.Second, "Polling interval (in seconds) for CRI")
	// Redis
	redisAddress      = flag.String("redis-address", "", "Address of the Redis service")
	redisLockTtl      = flag.Duration("redis-lock-ttl", 10*time.Second, "TTL of the Redis lock")
	redisLockInterval = flag.Duration("redis-lock-interval", 2*time.Second, "Interval (in seconds) of the Redis lock. The lock is held for this duration after the message is broadcasted")
)

func main() {
	klog.InitFlags(nil)
	defer klog.Flush()
	parseFlags()

	agent := agent.NewAgent(
		*targetPodUid,
		*targetContainerName,
		*criSocketPath,
		*criPollingInterval,
		*redisAddress,
		*redisLockTtl,
		*redisLockInterval,
	)
	ctx := context.Background()
	agent.Initialize(ctx)
	defer agent.Close()

	agent.Run(ctx)
}

func parseFlags() {
	klog.Infof("Parsing flags")
	flag.Parse()
	if *targetPodUid == "" {
		klog.Fatalf("Flag 'target-pod-uid' is required")
	}
	if *targetContainerName == "" {
		klog.Fatalf("Flag 'target-container-name' is required")
	}
	if *criSocketPath == "" {
		klog.Fatalf("Flag 'cri-socket-path' is required")
	}
	if *redisAddress == "" {
		klog.Fatalf("Flag 'redis-address' is required")
	}
}
