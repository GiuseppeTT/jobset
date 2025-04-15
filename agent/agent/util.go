package agent

import (
	"context"
	"fmt"
	"os"
	"strconv"

	"github.com/valkey-io/valkey-go"
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
	readyProbeHttpPath               = "/ready"
	readyProbePort                   = 80
)

type MessageData struct {
	DesiredTotalRestartCount int `json:"desiredTotalRestartCount"`
}

func intPtr(i int) *int {
	return &i
}

func (a *Agent) getBroadcastChannel(ctx context.Context) chan valkey.PubSubMessage {
	broadcastChannel := make(chan valkey.PubSubMessage)
	go func() {
		err := a.valkeyClient.Receive(ctx, a.valkeyClient.B().Subscribe().Channel(a.valkeyBroadcastChannelName).Build(), func(msg valkey.PubSubMessage) {
			broadcastChannel <- msg
		})
		if err != nil {
			klog.Errorf("Failed to receive broadcast message: %v", err)
		}
	}()
	return broadcastChannel
}

func (a *Agent) getCountVariablesFromValkey(ctx context.Context) (*int, *int, error) {
	rawRestartCount, rawTotalRestartCount, err := a.getRawCountVariablesFromValkey(ctx)
	if err != nil {
		return nil, nil, err
	}
	restartCount, totalRestartCount, err := a.parseCountVariables(rawRestartCount, rawTotalRestartCount)
	if err != nil {
		return nil, nil, err
	}
	return restartCount, totalRestartCount, nil
}

func (a *Agent) getRawCountVariablesFromValkey(ctx context.Context) (*string, *string, error) {
	script := valkey.NewLuaScript(`
	local value1 = server.call('HGET', KEYS[1], ARGV[1])
	local value2 = server.call('HGET', KEYS[2], ARGV[2])

	return { value1, value2 }
	`)
	keys := []string{a.buildValkeyRestartCountKeyName(), a.buildValkeyTotalRestartCountKeyName()}
	args := []string{a.workerId, a.workerId}
	list, err := script.Exec(ctx, a.valkeyClient, keys, args).ToArray()
	if err != nil {
		return nil, nil, fmt.Errorf("failed to get restart count and total restart count from Valkey: %w", err)
	}
	restartCount, err := list[0].ToString()
	if valkey.IsValkeyNil(err) {
		return nil, nil, nil
	}
	if err != nil {
		return nil, nil, fmt.Errorf("failed to extract value from restart count: %w", err)
	}
	totalRestartCount, err := list[1].ToString()
	if valkey.IsValkeyNil(err) {
		return nil, nil, nil
	}
	if err != nil {
		return nil, nil, fmt.Errorf("failed to extract value from total restart count: %w", err)
	}
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

func (a *Agent) initializeCountVariablesToValkey(ctx context.Context, restartCount *int, totalRestartCount *int) error {
	if restartCount == nil || totalRestartCount == nil {
		return fmt.Errorf("restart count and total restart count cannot be nil")
	}
	script := valkey.NewLuaScript(`
	local exists1 = server.call('HEXISTS', KEYS[1], ARGV[1])
	local exists2 = server.call('HEXISTS', KEYS[2], ARGV[2])

	if exists1 == 0 and exists2 == 0 then
		server.call('HSET', KEYS[1], ARGV[1], ARGV[3])
		server.call('HSET', KEYS[2], ARGV[2], ARGV[4])
		return server.status_reply("OK")
	else
		return server.error_reply("COND_NOT_MET: Field " .. ARGV[1] .. " in " .. KEYS[1] .. " or field " .. ARGV[2] .. " in " .. KEYS[2] .. " already exists")
	end
	`)
	keys := []string{a.buildValkeyRestartCountKeyName(), a.buildValkeyTotalRestartCountKeyName()}
	args := []string{a.workerId, a.workerId, strconv.Itoa(*restartCount), strconv.Itoa(*totalRestartCount)}
	err := script.Exec(ctx, a.valkeyClient, keys, args).Error()
	if err != nil {
		return fmt.Errorf("failed to initialize restart count and total restart count to Valkey: %w", err)
	}
	return nil
}

func (a *Agent) updateCountVariablesToValkey(ctx context.Context, restartCount *int, totalRestartCount *int) error {
	if restartCount == nil || totalRestartCount == nil {
		return fmt.Errorf("restart count and total restart count cannot be nil")
	}
	script := valkey.NewLuaScript(`
	local exists1 = server.call('HEXISTS', KEYS[1], ARGV[1])
	local exists2 = server.call('HEXISTS', KEYS[2], ARGV[2])

	if exists1 == 1 and exists2 == 1 then
		server.call('HSET', KEYS[1], ARGV[1], ARGV[3])
		server.call('HSET', KEYS[2], ARGV[2], ARGV[4])
		return server.status_reply("OK")
	else
		return server.error_reply("COND_NOT_MET: Field " .. ARGV[1] .. " in " .. KEYS[1] .. " or field " .. ARGV[2] .. " in " .. KEYS[2] .. " does not exist")
	end
	`)
	keys := []string{a.buildValkeyRestartCountKeyName(), a.buildValkeyTotalRestartCountKeyName()}
	args := []string{a.workerId, a.workerId, strconv.Itoa(*restartCount), strconv.Itoa(*totalRestartCount)}
	err := script.Exec(ctx, a.valkeyClient, keys, args).Error()
	if err != nil {
		return fmt.Errorf("failed to update restart count and total restart count to Valkey: %w", err)
	}
	return nil
}

func (a *Agent) buildValkeyRestartCountKeyName() string {
	return fmt.Sprintf("jobset-uid:%s:%s", a.workerJobsetUid, valkeyRestartCountKeySuffix)
}

func (a *Agent) buildValkeyTotalRestartCountKeyName() string {
	return fmt.Sprintf("jobset-uid:%s:%s", a.workerJobsetUid, valkeyTotalRestartCountKeySuffix)
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
