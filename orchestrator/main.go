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
	valkeyAddress   = flag.String("valkey-address", "", "Address of the Valkey service")
	pollingInterval = flag.Duration("polling-interval", -1, "Polling interval. The flag accepts a value acceptable to time.ParseDuration")
	podUid          = flag.String("pod-uid", "", "Pod UID")
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
		*pollingInterval,
		*podUid,
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
	if *pollingInterval < 0 {
		return fmt.Errorf("argument '--polling-interval' is required")
	}
	if *podUid == "" {
		return fmt.Errorf("argument '--pod-uid' is required")
	}
	return nil
}
