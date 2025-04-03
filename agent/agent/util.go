package agent

import (
	"context"
	"fmt"
	"os"
	"strconv"

	"github.com/redis/go-redis/v9"
	runtimeapi "k8s.io/cri-api/pkg/apis/runtime/v1"
	"k8s.io/klog/v2"
)

const (
	criPodUidLabelKey               = "io.kubernetes.pod.uid"
	criContainerNameLabelKey        = "io.kubernetes.container.name"
	criRestartCountAnnotationKey    = "io.kubernetes.container.restartCount"
	redisRestartCountKeySuffix      = "restart-count-by-worker-id"
	redisTotalRestartCountKeySuffix = "total-restart-count-by-worker-id"
	startedFilePath                 = "/app/startup/agent-started-once-before"
	readyProbeHttpPath              = "/ready"
	readyProbePort                  = 80
)

type MessageData struct {
	DesiredTotalRestartCount int `json:"desiredTotalRestartCount"`
}

func intPtr(i int) *int {
	return &i
}

func (a *Agent) getCountVariablesFromRedis(ctx context.Context) (*int, *int, error) {
	rawRestartCount, rawTotalRestartCount, err := a.getRawCountVariablesFromRedis(ctx)
	if err != nil {
		return nil, nil, err
	}
	restartCount, totalRestartCount, err := a.parseCountVariables(rawRestartCount, rawTotalRestartCount)
	if err != nil {
		return nil, nil, err
	}
	return restartCount, totalRestartCount, nil
}

func (a *Agent) getRawCountVariablesFromRedis(ctx context.Context) (*string, *string, error) {
	var responseRestartCount *redis.StringCmd
	var responseTotalRestartCount *redis.StringCmd
	_, err := a.redisClient.TxPipelined(ctx, func(transaction redis.Pipeliner) error {
		responseRestartCount = transaction.HGet(ctx, a.buildRedisRestartCountKeyName(), a.workerId)
		responseTotalRestartCount = transaction.HGet(ctx, a.buildRedisTotalRestartCountKeyName(), a.workerId)
		return nil
	})
	if err == redis.Nil {
		return nil, nil, nil
	}
	if err != nil {
		return nil, nil, fmt.Errorf("failed to get restart count and total restart count in transaction from Redis: %w", err)
	}
	restartCount := responseRestartCount.Val()
	totalRestartCount := responseTotalRestartCount.Val()
	return &restartCount, &totalRestartCount, nil
}

func (a *Agent) parseCountVariables(rawRestartCount *string, rawTotalRestartCount *string) (*int, *int, error) {
	if rawRestartCount == nil || rawTotalRestartCount == nil {
		return nil, nil, nil
	}
	restartCount, err := strconv.Atoi(*rawRestartCount)
	if err != nil {
		return nil, nil, fmt.Errorf("failed to convert restart count to integer: %w", err)
	}
	totalRestartCount, err := strconv.Atoi(*rawTotalRestartCount)
	if err != nil {
		return nil, nil, fmt.Errorf("failed to convert total restart count to integer: %w", err)
	}
	return &restartCount, &totalRestartCount, nil
}

func (a *Agent) initializeCountVariablesToRedis(ctx context.Context, restartCount *int, totalRestartCount *int) error {
	if restartCount == nil || totalRestartCount == nil {
		return fmt.Errorf("restart count and total restart count cannot be nil")
	}
	keys := []string{a.buildRedisRestartCountKeyName(), a.buildRedisTotalRestartCountKeyName()}
	args := []any{a.workerId, a.workerId, *restartCount, *totalRestartCount}
	_, err := hsetOnlyIfMissingScript.Run(ctx, a.redisClient, keys, args).Result()
	if err != nil {
		return fmt.Errorf("failed to initialize restart count and total restart count in transaction to Redis: %w", err)
	}
	return nil
}

var hsetOnlyIfMissingScript = redis.NewScript(`
local exists1 = redis.call('HEXISTS', KEYS[1], ARGV[1])
local exists2 = redis.call('HEXISTS', KEYS[2], ARGV[2])

if exists1 == 0 and exists2 == 0 then
	redis.call('HSET', KEYS[1], ARGV[1], ARGV[3])
	redis.call('HSET', KEYS[2], ARGV[2], ARGV[4])
	return redis.status_reply("OK")
else
	return redis.error_reply("COND_NOT_MET: Field " .. ARGV[1] .. " in " .. KEYS[1] .. " or field " .. ARGV[2] .. " in " .. KEYS[2] .. " already exists")
end
`)

func (a *Agent) updateCountVariablesToRedis(ctx context.Context, restartCount *int, totalRestartCount *int) error {
	if restartCount == nil || totalRestartCount == nil {
		return fmt.Errorf("restart count and total restart count cannot be nil")
	}
	keys := []string{a.buildRedisRestartCountKeyName(), a.buildRedisTotalRestartCountKeyName()}
	args := []any{a.workerId, a.workerId, *restartCount, *totalRestartCount}
	_, err := hsetOnlyIfExistScript.Run(ctx, a.redisClient, keys, args).Result()
	if err != nil {
		return fmt.Errorf("failed to update restart count and total restart count in transaction to Redis: %w", err)
	}
	return nil
}

var hsetOnlyIfExistScript = redis.NewScript(`
local exists1 = redis.call('HEXISTS', KEYS[1], ARGV[1])
local exists2 = redis.call('HEXISTS', KEYS[2], ARGV[2])

if exists1 == 1 and exists2 == 1 then
	redis.call('HSET', KEYS[1], ARGV[1], ARGV[3])
	redis.call('HSET', KEYS[2], ARGV[2], ARGV[4])
  return redis.status_reply("OK")
else
  	return redis.error_reply("COND_NOT_MET: Field " .. ARGV[1] .. " in " .. KEYS[1] .. " or field " .. ARGV[2] .. " in " .. KEYS[2] .. " does not exist")
end
`)

func (a *Agent) buildRedisRestartCountKeyName() string {
	return fmt.Sprintf("jobset-uid:%s:%s", a.workerJobsetUid, redisRestartCountKeySuffix)
}

func (a *Agent) buildRedisTotalRestartCountKeyName() string {
	return fmt.Sprintf("jobset-uid:%s:%s", a.workerJobsetUid, redisTotalRestartCountKeySuffix)
}

func (a *Agent) getContainer(ctx context.Context, podUid string, containerName string) (*runtimeapi.Container, error) {
	listResponse, err := a.criClient.ListContainers(ctx, &runtimeapi.ListContainersRequest{
		Filter: &runtimeapi.ContainerFilter{
			State: &runtimeapi.ContainerStateValue{
				State: runtimeapi.ContainerState_CONTAINER_RUNNING,
			},
			LabelSelector: map[string]string{
				criPodUidLabelKey:        podUid,
				criContainerNameLabelKey: containerName,
			},
		},
	})
	if err != nil {
		return nil, fmt.Errorf("failed to list containers: %w", err)
	}
	if len(listResponse.Containers) == 0 {
		return nil, fmt.Errorf("no running containers found")
	}
	if len(listResponse.Containers) > 1 {
		return nil, fmt.Errorf("more than one running container found")
	}
	return listResponse.Containers[0], nil
}

func (a *Agent) killContainer(ctx context.Context, id string) error {
	_, err := a.criClient.StopContainer(ctx, &runtimeapi.StopContainerRequest{
		ContainerId: id,
		Timeout:     int64(a.workerTerminationPeriod.Seconds()),
	})
	if err != nil {
		return fmt.Errorf("failed to kill container: %w", err)
	}
	return nil
}

func (a *Agent) startedFileExists() (bool, error) {
	klog.Infof("Checking if started file exists")
	_, err := os.Stat(startedFilePath)
	if err == nil {
		return true, nil
	}
	if os.IsNotExist(err) {
		return false, nil
	}
	return false, fmt.Errorf("failed to check if started file exists: %w", err)
}

func (a *Agent) createStartedFile() error {
	klog.Infof("Creating started file")
	file, err := os.Create(startedFilePath)
	if err != nil {
		return fmt.Errorf("failed to create started file: %w", err)
	}
	defer file.Close()
	return nil
}
