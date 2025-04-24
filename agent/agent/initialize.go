package agent

import (
	"context"
	"fmt"

	"k8s.io/klog/v2"
)

func (a *Agent) HandleInitializationCases(ctx context.Context) error {
	klog.Info("Handling initialization cases")
	restartCount, totalRestartCount, err := a.getCountVariablesFromValkey(ctx)
	if err != nil {
		return err
	}
	if totalRestartCount == nil {
		return a.handlePodCreatedCase(ctx)
	}
	if *totalRestartCount < 0 {
		return fmt.Errorf("total restart count cannot be negative")
	}
	startedFileExists, err := a.startedFileExists()
	if err != nil {
		return err
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
	err := a.createStartedFile()
	if err != nil {
		return err
	}
	err = a.initializeCountVariablesToValkey(ctx, newRestartCount, newTotalRestartCount)
	if err != nil {
		return err
	}
	a.restartCount = newRestartCount
	a.totalRestartCount = newTotalRestartCount
	return nil
}

func (a *Agent) handlePodCreatedIndepententlyCase(ctx context.Context, valkeyTotalRestartCount *int) error {
	klog.Info("Handling pod created independently case")
	newRestartCount := intPtr(0)
	newTotalRestartCount := intPtr(*valkeyTotalRestartCount + 1)
	err := a.updateCountVariablesToValkey(ctx, newRestartCount, newTotalRestartCount)
	if err != nil {
		return err
	}
	err = a.createStartedFile()
	if err != nil {
		return err
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
