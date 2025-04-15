package agent

import (
	"context"
	"fmt"
	"net/http"

	"k8s.io/klog/v2"

	"github.com/valkey-io/valkey-go"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/local"
	runtimeapi "k8s.io/cri-api/pkg/apis/runtime/v1"
)

func (a *Agent) Initialize(ctx context.Context) error {
	klog.Infof("Initializing agent")
	err := a.connectToCri()
	if err != nil {
		return fmt.Errorf("failed to connect to CRI: %w", err)
	}
	err = a.connectToValkey(ctx)
	if err != nil {
		return fmt.Errorf("failed to connect to Valkey: %w", err)
	}
	err = a.handleInitializationCases(ctx)
	if err != nil {
		return fmt.Errorf("failed to handle initialization cases: %w", err)
	}
	go a.serveReadyProbe()
	return nil
}

func (a *Agent) connectToCri() error {
	klog.Infof("Connecting to CRI")
	criCon, err := grpc.NewClient(a.criSocketPath, grpc.WithTransportCredentials(local.NewCredentials()))
	if err != nil {
		return fmt.Errorf("failed to create gRCP client: %w", err)
	}
	criClient := runtimeapi.NewRuntimeServiceClient(criCon)
	a.criCon = criCon
	a.criClient = criClient
	return nil
}

func (a *Agent) connectToValkey(ctx context.Context) error {
	klog.Infof("Connecting to Valkey")
	valkeyClient, err := valkey.NewClient(valkey.ClientOption{InitAddress: []string{a.valkeyAddress}})
	if err != nil {
		return fmt.Errorf("failed to create Valkey client: %w", err)
	}
	err = valkeyClient.Do(ctx, valkeyClient.B().Ping().Build()).Error()
	if err != nil {
		return fmt.Errorf("failed to ping Valkey: %w", err)
	}
	a.valkeyClient = valkeyClient
	return nil
}

func (a *Agent) handleInitializationCases(ctx context.Context) error {
	klog.Infof("Handling initialization cases")
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
	klog.Infof("Handling pod created case")
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
	klog.Infof("Handling pod created independently case")
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
	klog.Infof("Handling agent restarted case")
	a.restartCount = valkeyRestartCount
	a.totalRestartCount = valkeyTotalRestartCount
	return nil
}

func (a *Agent) serveReadyProbe() {
	klog.Infof("Serving ready probe")
	http.HandleFunc(readyProbeHttpPath, func(w http.ResponseWriter, r *http.Request) {})
	address := fmt.Sprintf(":%d", readyProbePort)
	err := http.ListenAndServe(address, nil)
	if err != nil {
		klog.Errorf("Failed to serve ready probe: %v", err)
	}
}

func (a *Agent) Close() {
	klog.Infof("Closing agent")
	a.criCon.Close()
	a.valkeyClient.Close()
}
