package agent

import (
	"context"
	"fmt"
	"time"

	"github.com/valkey-io/valkey-go"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/local"
	runtimeapi "k8s.io/cri-api/pkg/apis/runtime/v1"
	"k8s.io/klog/v2"
)

const (
	criPodUidLabelKey                = "io.kubernetes.pod.uid"
	criContainerNameLabelKey         = "io.kubernetes.container.name"
	criRestartCountAnnotationKey     = "io.kubernetes.container.restartCount"
	valkeyRestartCountKeySuffix      = "restart-count-by-worker-id"
	valkeyTotalRestartCountKeySuffix = "total-restart-count-by-worker-id"
	startedFilePath                  = "/app/startup/agent-started-once-before"
)

type Agent struct {
	workerJobsetUid            string
	workerId                   string
	workerPodUid               string
	workerContainerName        string
	workerTerminationPeriod    time.Duration
	criSocketPath              string
	criPollingInterval         time.Duration
	criCon                     *grpc.ClientConn
	criClient                  runtimeapi.RuntimeServiceClient
	valkeyAddress              string
	valkeyBroadcastChannelName string
	valkeyClient               valkey.Client
	restartCount               *int
	totalRestartCount          *int
	desiredTotalRestartCount   *int
}

func NewAgent(
	workerJobsetUid string,
	workerId string,
	workerPodUid string,
	workerContainerName string,
	workerTerminationPeriod time.Duration,
	criSocketPath string,
	criPollingInterval time.Duration,
	valkeyAddress string,
	valkeyBroadcastChannelName string,
) (*Agent, error) {
	klog.Info("Creating agent")
	criCon, criClient, err := connectToCri(criSocketPath)
	if err != nil {
		return nil, fmt.Errorf("failed to connect to CRI: %w", err)
	}
	valkeyClient, err := connectToValkey(valkeyAddress)
	if err != nil {
		return nil, fmt.Errorf("failed to connect to Valkey: %w", err)
	}
	return &Agent{
		workerJobsetUid:            workerJobsetUid,
		workerId:                   workerId,
		workerPodUid:               workerPodUid,
		workerContainerName:        workerContainerName,
		workerTerminationPeriod:    workerTerminationPeriod,
		criSocketPath:              criSocketPath,
		criPollingInterval:         criPollingInterval,
		criCon:                     criCon,
		criClient:                  criClient,
		valkeyAddress:              valkeyAddress,
		valkeyBroadcastChannelName: valkeyBroadcastChannelName,
		valkeyClient:               valkeyClient,
		restartCount:               nil,
		totalRestartCount:          nil,
	}, nil
}

func connectToCri(socketPath string) (*grpc.ClientConn, runtimeapi.RuntimeServiceClient, error) {
	klog.Info("Connecting to CRI")
	criCon, err := grpc.NewClient(socketPath, grpc.WithTransportCredentials(local.NewCredentials()))
	if err != nil {
		return nil, nil, fmt.Errorf("failed to create gRCP client: %w", err)
	}
	criClient := runtimeapi.NewRuntimeServiceClient(criCon)
	return criCon, criClient, nil
}

func connectToValkey(address string) (valkey.Client, error) {
	klog.Info("Connecting to Valkey")
	valkeyClient, err := valkey.NewClient(valkey.ClientOption{InitAddress: []string{address}})
	if err != nil {
		return nil, fmt.Errorf("failed to create Valkey client: %w", err)
	}
	err = valkeyClient.Do(context.Background(), valkeyClient.B().Ping().Build()).Error()
	if err != nil {
		return nil, fmt.Errorf("failed to ping Valkey: %w", err)
	}
	return valkeyClient, nil
}

func (a *Agent) Close() {
	klog.Info("Closing agent")
	a.criCon.Close()
	a.valkeyClient.Close()
}
