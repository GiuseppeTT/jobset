package agent

import (
	"time"

	"github.com/redis/go-redis/v9"
	"google.golang.org/grpc"
	runtimeapi "k8s.io/cri-api/pkg/apis/runtime/v1"
)

type Agent struct {
	workerJobsetUid           string
	workerId                  string
	workerPodUid              string
	workerContainerName       string
	workerTerminationPeriod   time.Duration
	criSocketPath             string
	criPollingInterval        time.Duration
	criCon                    *grpc.ClientConn
	criClient                 runtimeapi.RuntimeServiceClient
	redisAddress              string
	redisBroadcastChannelName string
	redisClient               *redis.Client
	redisSubscription         *redis.PubSub
	restartCount              *int
	totalRestartCount         *int
}

func NewAgent(
	workerJobsetUid string,
	workerId string,
	workerPodUid string,
	workerContainerName string,
	workerTerminationPeriod time.Duration,
	criSocketPath string,
	criPollingInterval time.Duration,
	redisAddress string,
	redisBroadcastChannelName string,
) *Agent {
	return &Agent{
		workerJobsetUid:           workerJobsetUid,
		workerId:                  workerId,
		workerPodUid:              workerPodUid,
		workerContainerName:       workerContainerName,
		workerTerminationPeriod:   workerTerminationPeriod,
		criSocketPath:             criSocketPath,
		criPollingInterval:        criPollingInterval,
		criCon:                    nil,
		criClient:                 nil,
		redisAddress:              redisAddress,
		redisBroadcastChannelName: redisBroadcastChannelName,
		redisClient:               nil,
		redisSubscription:         nil,
		restartCount:              nil,
		totalRestartCount:         nil,
	}
}
