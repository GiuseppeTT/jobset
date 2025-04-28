package agent

import (
	"context"
	"fmt"

	"k8s.io/klog/v2"
)

func (a *Agent) HandleInitializationCases() error {
	klog.Info("Handling initialization cases")
	ctx := context.Background()
	restartCount, totalRestartCount, err := a.getCountsFromValkey(ctx)
	if err != nil {
		return fmt.Errorf("failed to get counts from Valkey: %w", err)
	}
	if totalRestartCount == nil {
		return a.handlePodCreatedCase(ctx)
	}
	if *totalRestartCount < 0 {
		return fmt.Errorf("total restart count cannot be negative")
	}
	startedFileExists, err := a.startedFileExists()
	if err != nil {
		return fmt.Errorf("failed to check if started file exists: %w", err)
	}
	if !startedFileExists {
		return a.handlePodCreatedIndepententlyCase(ctx, totalRestartCount)
	}
	return a.handleAgentRestartedCase(restartCount, totalRestartCount)
}

func (a *Agent) handlePodCreatedCase(ctx context.Context) error {
	klog.Info("Handling pod created case")
	newRestartCount := intPtr(0)
	newTotalRestartCount := intPtr(0)
	// It is ok to create the started file before initializing counts to Valkey
	// because a failure between these two operations can be handled. If this
	// happens, the new agent process will detect that the counts are not in
	// Valkey and execute the "pod created" case again, even if the started
	// file exists
	err := a.createStartedFile()
	if err != nil {
		return fmt.Errorf("failed to create started file: %w", err)
	}
	err = a.initializeCountsToValkey(ctx, newRestartCount, newTotalRestartCount)
	if err != nil {
		return fmt.Errorf("failed to initialize counts to Valkey: %w", err)
	}
	a.restartCount = newRestartCount
	a.totalRestartCount = newTotalRestartCount
	return nil
}

func (a *Agent) handlePodCreatedIndepententlyCase(ctx context.Context, valkeyTotalRestartCount *int) error {
	klog.Info("Handling pod created independently case")
	newRestartCount := intPtr(0)
	newTotalRestartCount := intPtr(*valkeyTotalRestartCount + 1)
	// This case requires to update the counts to Valkey before creating the
	// started file. If a failure happens between these two operations, the
	// new agent process will detect that the counts exist in Valkey and the
	// started file does not exist and execute the "pod created independently"
	// case again. Technically the total restart count will be increased two
	// times, but the orchestrator can handle that (by probably failing over
	// to full recreation)
	err := a.updateCountsToValkey(ctx, newRestartCount, newTotalRestartCount)
	if err != nil {
		return fmt.Errorf("failed to update counts to Valkey: %w", err)
	}
	err = a.createStartedFile()
	if err != nil {
		return fmt.Errorf("failed to create started file: %w", err)
	}
	a.restartCount = newRestartCount
	a.totalRestartCount = newTotalRestartCount
	return nil
}

func (a *Agent) handleAgentRestartedCase(valkeyRestartCount *int, valkeyTotalRestartCount *int) error {
	klog.Info("Handling agent restarted case")
	a.restartCount = valkeyRestartCount
	a.totalRestartCount = valkeyTotalRestartCount
	return nil
}
