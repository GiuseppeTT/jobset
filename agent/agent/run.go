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

func (a *Agent) Run(ctx context.Context) {
	klog.Info("Running agent")
	ticker := time.NewTicker(a.criPollingInterval)
	defer ticker.Stop()
	broadcastChannel := a.getBroadcastChannel(ctx)
	defer close(broadcastChannel)
	for {
		select {
		case <-ticker.C:
			a.pollWorkerContainer(ctx)
			a.handleRequestForRestart(ctx)
		case message := <-broadcastChannel:
			a.handleBroadcastMessage(ctx, message)
		case <-ctx.Done():
			return
		}
	}
}

func (a *Agent) pollWorkerContainer(ctx context.Context) {
	klog.V(2).Info("Polling worker container")
	if *a.restartCount < 0 || *a.totalRestartCount < 0 {
		klog.V(2).Infof("Stored counts are negative. Setting request to full recreation")
		a.setRequestToFullRecreation(ctx)
		return
	}
	workerContainer, err := a.getContainer(ctx, a.workerPodUid, a.workerContainerName)
	if err != nil {
		klog.Errorf("Failed to get worker container: %v", err)
		return
	}
	rawRestartCount, ok := workerContainer.Annotations[criRestartCountAnnotationKey]
	if !ok {
		klog.Error("Restart count annotation not found")
		return
	}
	restartCount, err := strconv.Atoi(rawRestartCount)
	if err != nil {
		klog.Errorf("Failed to convert restart count to integer: %v", err)
		return
	}
	klog.V(2).Infof("Polled restart count: %d", restartCount)
	if restartCount == *a.restartCount {
		klog.V(2).Info("No restart detected")
		return
	}
	if restartCount == *a.restartCount+1 {
		a.handleWorkerRestartedCase(ctx)
		return
	}
	a.setRequestToFullRecreation(ctx)
}

func (a *Agent) handleWorkerRestartedCase(ctx context.Context) {
	klog.Info("Handling worker restarted case")
	newRestartCount := intPtr(*a.restartCount + 1)
	newTotalRestartCount := intPtr(*a.totalRestartCount + 1)
	err := a.updateCountVariablesToValkey(ctx, newRestartCount, newTotalRestartCount)
	if err != nil {
		klog.Errorf("Failed to update restart count and total restart count in transaction to Valkey: %v", err)
		return
	}
	a.restartCount = newRestartCount
	a.totalRestartCount = newTotalRestartCount
}

func (a *Agent) handleRequestForRestart(ctx context.Context) {
	if *a.restartCount < 0 || *a.totalRestartCount < 0 {
		klog.V(2).Infof("Stored counts are negative. Setting request to full recreation")
		a.setRequestToFullRecreation(ctx)
		return
	}
	if a.desiredTotalRestartCount == nil {
		return
	}
	klog.V(2).Info("Handling request for restart")
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
	a.setRequestToFullRecreation(ctx)
}

func (a *Agent) killWorkerContainer(ctx context.Context) error {
	klog.Infof("Killing worker container")
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

func (a *Agent) handleBroadcastMessage(ctx context.Context, message valkey.PubSubMessage) {
	klog.Info("Handling broadcast message")
	if *a.restartCount < 0 || *a.totalRestartCount < 0 {
		klog.V(2).Info("Stored counts are negative. Setting request to full recreation")
		a.setRequestToFullRecreation(ctx)
		return
	}
	var messageData MessageData
	err := json.Unmarshal([]byte(message.Message), &messageData)
	if err != nil {
		klog.Errorf("Failed to unmarshall broadcast message data from JSON: %v", err)
		return
	}
	klog.Infof("Desired total restart count: %d", messageData.DesiredTotalRestartCount)
	a.desiredTotalRestartCount = intPtr(messageData.DesiredTotalRestartCount)
}

func (a *Agent) setRequestToFullRecreation(ctx context.Context) {
	klog.Info("Setting request to full recreation")
	newRestartCount := intPtr(-999)
	newTotalRestartCount := intPtr(-999)
	a.restartCount = newRestartCount
	a.totalRestartCount = newTotalRestartCount
	err := a.updateCountVariablesToValkey(ctx, newRestartCount, newTotalRestartCount)
	if err != nil {
		klog.Errorf("Failed to update restart count and total restart count in transaction to Valkey: %v", err)
		return
	}
}
