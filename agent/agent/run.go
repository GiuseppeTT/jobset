package agent

import (
	"context"
	"encoding/json"
	"fmt"
	"strconv"
	"time"

	"github.com/valkey-io/valkey-go"
	"k8s.io/klog/v2"
)

const (
	badRestartCountValue      = -999
	badTotalRestartCountvalue = -999
)

func (a *Agent) Run(ctx context.Context) {
	klog.Info("Running agent")
	ticker := time.NewTicker(a.criPollingInterval)
	defer ticker.Stop()
	broadcastChannel := a.getBroadcastChannel(ctx)
	defer close(broadcastChannel)
	for {
		// If recreation is requested once, keep handling it until the
		// orchestrator recreates all pods
		if a.isFullRecreationRequested() {
			a.handleRequestForFullRecreation(ctx)
			continue
		}
		select {
		case <-ticker.C:
			a.pollWorkerContainer(ctx)
			// Optimization
			// Handle request for restart right after a poll to ensure
			// restart count is as fresh as possible
			if a.isRestartRequested() {
				a.handleRequestForRestart(ctx)
			}
		case message := <-broadcastChannel:
			a.handleBroadcastMessage(message)
		case <-ctx.Done():
			return
		}
	}
}

func (a *Agent) pollWorkerContainer(ctx context.Context) {
	klog.V(2).Info("Polling worker container")
	workerContainer, err := a.getContainer(ctx, a.workerPodUid, a.workerContainerName)
	if err != nil {
		klog.Errorf("Failed to get worker container: %v", err)
		return
	}
	rawRestartCount, ok := workerContainer.Annotations[criRestartCountAnnotationKey]
	if !ok {
		klog.Error("Failed to get restart count annotation from container")
		return
	}
	restartCount, err := strconv.Atoi(rawRestartCount)
	if err != nil {
		klog.Errorf("Failed to convert raw restart count '%s' to integer: %v", rawRestartCount, err)
		return
	}
	klog.V(2).Infof("Polled restart count: %d", restartCount)
	if restartCount == *a.restartCount {
		klog.V(2).Info("No restart detected")
		return
	}
	if restartCount == *a.restartCount+1 {
		klog.Info("Single restart detected")
		a.handleWorkerRestartedCase(ctx)
		return
	}
	klog.Errorf("Invalid combination detected. Stored restart count is '%d' and polled restart count is '%d'. Requesting full recreation", *a.restartCount, restartCount)
	a.requestFullRecreation()
}

func (a *Agent) handleWorkerRestartedCase(ctx context.Context) {
	klog.Info("Handling worker restarted case")
	newRestartCount := intPtr(*a.restartCount + 1)
	newTotalRestartCount := intPtr(*a.totalRestartCount + 1)
	err := a.updateCountsToValkey(ctx, newRestartCount, newTotalRestartCount)
	if err != nil {
		klog.Errorf("Failed to update restart count and total restart count to Valkey: %v", err)
		return
	}
	a.restartCount = newRestartCount
	a.totalRestartCount = newTotalRestartCount
}

func (a *Agent) handleBroadcastMessage(message valkey.PubSubMessage) {
	klog.Info("Handling broadcast message")
	var messageData MessageData
	err := json.Unmarshal([]byte(message.Message), &messageData)
	if err != nil {
		klog.Errorf("Failed to unmarshall broadcast message data from JSON: %v", err)
		return
	}
	klog.Infof("Desired total restart count: %d", messageData.DesiredTotalRestartCount)
	a.requestRestart(messageData.DesiredTotalRestartCount)
}

func (a *Agent) requestRestart(newDesiredTotalRestartCount int) {
	klog.Info("Requesting restart")
	a.desiredTotalRestartCount = intPtr(newDesiredTotalRestartCount)
}

func (a *Agent) isRestartRequested() bool {
	klog.V(2).Info("Checking if restart is requested")
	return !a.isFullRecreationRequested() && a.desiredTotalRestartCount != nil
}

func (a *Agent) handleRequestForRestart(ctx context.Context) {
	klog.Info("Handling request for restart")
	if *a.desiredTotalRestartCount == *a.totalRestartCount {
		klog.Infof("Restart not required. Desired total restart count is the same as the current total restart count")
		a.desiredTotalRestartCount = nil
		return
	}
	if *a.desiredTotalRestartCount == *a.totalRestartCount+1 {
		err := a.killWorkerContainer(ctx)
		if err != nil {
			klog.Errorf("Failed to kill worker container: %v", err)
			return
		}
		a.desiredTotalRestartCount = nil
		return
	}
	klog.Errorf("Invalid combination detected. Stored total restart count is '%d' and desired total restart count is '%d'. Requesting full recreation", *a.totalRestartCount, *a.desiredTotalRestartCount)
	a.requestFullRecreation()
}

func (a *Agent) killWorkerContainer(ctx context.Context) error {
	klog.Info("Killing worker container")
	workerContainer, err := a.getContainer(ctx, a.workerPodUid, a.workerContainerName)
	if err != nil {
		return fmt.Errorf("failed to get worker container: %v", err)
	}
	err = a.killContainer(ctx, workerContainer.Id)
	if err != nil {
		return fmt.Errorf("failed to kill worker container: %v", err)
	}
	return nil
}

func (a *Agent) requestFullRecreation() {
	klog.Info("Requesting full recreation")
	a.restartCount = intPtr(badRestartCountValue)
	a.totalRestartCount = intPtr(badTotalRestartCountvalue)
}

func (a *Agent) isFullRecreationRequested() bool {
	klog.V(2).Info("Checking if full recreation is requested")
	return (a.restartCount != nil && *a.restartCount < 0) || (a.totalRestartCount != nil && *a.totalRestartCount < 0)
}

func (a *Agent) handleRequestForFullRecreation(ctx context.Context) {
	klog.Info("Handling request for full recreation")
	badRestartCount := intPtr(badRestartCountValue)
	badTotalRestartCount := intPtr(badTotalRestartCountvalue)
	err := a.updateCountsToValkey(ctx, badRestartCount, badTotalRestartCount)
	if err != nil {
		klog.Errorf("Failed to update restart count and total restart count to bad values to Valkey: %v", err)
		return
	}
}
