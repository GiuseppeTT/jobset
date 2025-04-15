package agent

import (
	"time"

	"github.com/valkey-io/valkey-go"
	"google.golang.org/grpc"
	runtimeapi "k8s.io/cri-api/pkg/apis/runtime/v1"
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
) *Agent {
	return &Agent{
		workerJobsetUid:            workerJobsetUid,
		workerId:                   workerId,
		workerPodUid:               workerPodUid,
		workerContainerName:        workerContainerName,
		workerTerminationPeriod:    workerTerminationPeriod,
		criSocketPath:              criSocketPath,
		criPollingInterval:         criPollingInterval,
		criCon:                     nil,
		criClient:                  nil,
		valkeyAddress:              valkeyAddress,
		valkeyBroadcastChannelName: valkeyBroadcastChannelName,
		valkeyClient:               nil,
		restartCount:               nil,
		totalRestartCount:          nil,
	}
}
