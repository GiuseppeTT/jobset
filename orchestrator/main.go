package main

import (
	"context"
	"flag"
	"fmt"
	"orchestrator/orchestrator"
	"os"

	"k8s.io/klog/v2"
)

var (
	valkeyAddress              = flag.String("valkey-address", "", "Address of the Valkey service")
	valkeyBroadcastChannelName = flag.String("valkey-broadcast-channel-name", "", "Name of the broadcast channel")
	valkeyPollingInterval      = flag.Duration("valkey-polling-interval", -1, "Polling interval for the CRI API")
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

	orchestrator, err := orchestrator.NewOrchestrator(
		*valkeyAddress,
		*valkeyBroadcastChannelName,
		*valkeyPollingInterval,
	)
	if err != nil {
		klog.Errorf("Failed to create orchestrator: %v", err)
		os.Exit(1)
	}
	defer orchestrator.Close()

	ctx := context.Background()
	orchestrator.Run(ctx)
}

func validateFlags() error {
	klog.Info("Validating flags")
	if *valkeyAddress == "" {
		return fmt.Errorf("argument '--valkey-address' is required")
	}
	if *valkeyBroadcastChannelName == "" {
		return fmt.Errorf("argument '--valkey-broadcast-channel-name' is required")
	}
	if *valkeyPollingInterval < 0 {
		return fmt.Errorf("argument '--valkey-polling-interval' is required")
	}
	return nil
}
