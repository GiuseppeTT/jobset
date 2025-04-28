package agent

import (
	"fmt"
	"time"

	"github.com/valkey-io/valkey-go"
	"google.golang.org/grpc"
	runtimeapi "k8s.io/cri-api/pkg/apis/runtime/v1"
	"k8s.io/klog/v2"
)

type Agent struct {
	workerJobSetUid              string
	workerId                     string
	workerPodUid                 string
	workerContainerName          string
	workerTerminationGracePeriod time.Duration
	criSocketPath                string
	criPollingInterval           time.Duration
	criCon                       *grpc.ClientConn
	criClient                    runtimeapi.RuntimeServiceClient
	valkeyAddress                string
	valkeyClient                 valkey.Client
	restartCount                 *int
	totalRestartCount            *int
	desiredTotalRestartCount     *int
}

func NewAgent(
	workerJobSetUid string,
	workerId string,
	workerPodUid string,
	workerContainerName string,
	workerTerminationGracePeriod time.Duration,
	criSocketPath string,
	criPollingInterval time.Duration,
	valkeyAddress string,
) (*Agent, error) {
	klog.Info("Creating new agent")
	criCon, criClient, err := connectToCri(criSocketPath)
	if err != nil {
		return nil, fmt.Errorf("failed to connect to CRI: %w", err)
	}
	valkeyClient, err := connectToValkey(valkeyAddress)
	if err != nil {
		return nil, fmt.Errorf("failed to connect to Valkey: %w", err)
	}
	return &Agent{
		workerJobSetUid:              workerJobSetUid,
		workerId:                     workerId,
		workerPodUid:                 workerPodUid,
		workerContainerName:          workerContainerName,
		workerTerminationGracePeriod: workerTerminationGracePeriod,
		criSocketPath:                criSocketPath,
		criPollingInterval:           criPollingInterval,
		criCon:                       criCon,
		criClient:                    criClient,
		valkeyAddress:                valkeyAddress,
		valkeyClient:                 valkeyClient,
		restartCount:                 nil,
		totalRestartCount:            nil,
		desiredTotalRestartCount:     nil,
	}, nil
}

func (a *Agent) Close() {
	klog.Info("Closing agent")
	a.criCon.Close()
	a.valkeyClient.Close()
}
