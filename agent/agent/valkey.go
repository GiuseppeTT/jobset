package agent

import (
	"context"
	"fmt"
	"strconv"

	"github.com/valkey-io/valkey-go"
	"k8s.io/klog/v2"
)

type MessageData struct {
	DesiredTotalRestartCount int `json:"desiredTotalRestartCount"`
}

const (
	valkeyJobSetUidKeyPreffix        = "jobset-uid"
	valkeyRestartCountKeySuffix      = "restart-count-by-worker-id"
	valkeyTotalRestartCountKeySuffix = "total-restart-count-by-worker-id"
)

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

func (a *Agent) getBroadcastChannel(ctx context.Context) chan valkey.PubSubMessage {
	klog.Info("Getting broadcast channel")
	broadcastChannel := make(chan valkey.PubSubMessage)
	go func() {
		err := a.valkeyClient.Receive(ctx, a.valkeyClient.B().Subscribe().Channel(a.buildValkeyBroadcastChannelName()).Build(), func(msg valkey.PubSubMessage) {
			broadcastChannel <- msg
		})
		if err != nil {
			klog.Errorf("Failed to receive broadcast message: %v", err)
		}
	}()
	return broadcastChannel
}

func (a *Agent) getCountsFromValkey(ctx context.Context) (*int, *int, error) {
	klog.Info("Getting counts from Valkey")
	rawRestartCount, rawTotalRestartCount, err := a.getRawCountsFromValkey(ctx)
	if err != nil {
		return nil, nil, fmt.Errorf("failed to get raw counts from Valkey: %w", err)
	}
	restartCount, totalRestartCount, err := a.parseCounts(rawRestartCount, rawTotalRestartCount)
	if err != nil {
		return nil, nil, fmt.Errorf("failed to parse counts: %w", err)
	}
	return restartCount, totalRestartCount, nil
}

func (a *Agent) getRawCountsFromValkey(ctx context.Context) (*string, *string, error) {
	script := valkey.NewLuaScript(`
	local value1 = server.call('HGET', KEYS[1], ARGV[1])
	local value2 = server.call('HGET', KEYS[2], ARGV[2])

	return { value1, value2 }
	`)
	keys := []string{a.buildValkeyRestartCountKey(), a.buildValkeyTotalRestartCountKey()}
	args := []string{a.workerId, a.workerId}
	list, err := script.Exec(ctx, a.valkeyClient, keys, args).ToArray()
	if err != nil {
		return nil, nil, fmt.Errorf("failed to get restart count and total restart count from Valkey with Lua script: %w", err)
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

func (a *Agent) parseCounts(rawRestartCount *string, rawTotalRestartCount *string) (*int, *int, error) {
	if rawRestartCount == nil || rawTotalRestartCount == nil {
		return nil, nil, nil
	}
	restartCount, err := strconv.Atoi(*rawRestartCount)
	if err != nil {
		return nil, nil, fmt.Errorf("failed to convert raw restart count '%s' to integer: %w", *rawRestartCount, err)
	}
	totalRestartCount, err := strconv.Atoi(*rawTotalRestartCount)
	if err != nil {
		return nil, nil, fmt.Errorf("failed to convert raw total restart count '%s' to integer: %w", *rawTotalRestartCount, err)
	}
	return &restartCount, &totalRestartCount, nil
}

func (a *Agent) initializeCountsToValkey(ctx context.Context, restartCount *int, totalRestartCount *int) error {
	klog.Info("Initializing counts to Valkey")
	if restartCount == nil || totalRestartCount == nil {
		return fmt.Errorf("restart count and total restart count cannot be nil")
	}
	script := valkey.NewLuaScript(`
	local exists1 = server.call('HEXISTS', KEYS[1], ARGV[1])
	local exists2 = server.call('HEXISTS', KEYS[2], ARGV[2])

	if exists1 == 0 and exists2 == 0 then
		server.call('HSET', KEYS[1], ARGV[1], ARGV[3])
		server.call('HSET', KEYS[2], ARGV[2], ARGV[4])
	else
		return server.error_reply("COND_NOT_MET: Field " .. ARGV[1] .. " in " .. KEYS[1] .. " or field " .. ARGV[2] .. " in " .. KEYS[2] .. " already exists")
	end
	`)
	keys := []string{a.buildValkeyRestartCountKey(), a.buildValkeyTotalRestartCountKey()}
	parsedRestartCount := strconv.Itoa(*restartCount)
	parsedTotalRestartCount := strconv.Itoa(*totalRestartCount)
	args := []string{a.workerId, a.workerId, parsedRestartCount, parsedTotalRestartCount}
	err := script.Exec(ctx, a.valkeyClient, keys, args).Error()
	if err != nil && !valkey.IsValkeyNil(err) {
		return fmt.Errorf("failed to initialize restart count '%s' and total restart count '%s' to Valkey with Lua script: %w", parsedRestartCount, parsedTotalRestartCount, err)
	}
	return nil
}

func (a *Agent) updateCountsToValkey(ctx context.Context, restartCount *int, totalRestartCount *int) error {
	klog.Info("Updating counts to Valkey")
	if restartCount == nil || totalRestartCount == nil {
		return fmt.Errorf("restart count and total restart count cannot be nil")
	}
	script := valkey.NewLuaScript(`
	local exists1 = server.call('HEXISTS', KEYS[1], ARGV[1])
	local exists2 = server.call('HEXISTS', KEYS[2], ARGV[2])

	if exists1 == 1 and exists2 == 1 then
		server.call('HSET', KEYS[1], ARGV[1], ARGV[3])
		server.call('HSET', KEYS[2], ARGV[2], ARGV[4])
	else
		return server.error_reply("COND_NOT_MET: Field " .. ARGV[1] .. " in " .. KEYS[1] .. " or field " .. ARGV[2] .. " in " .. KEYS[2] .. " does not exist")
	end
	`)
	keys := []string{a.buildValkeyRestartCountKey(), a.buildValkeyTotalRestartCountKey()}
	parsedRestartCount := strconv.Itoa(*restartCount)
	parsedTotalRestartCount := strconv.Itoa(*totalRestartCount)
	args := []string{a.workerId, a.workerId, parsedRestartCount, parsedTotalRestartCount}
	err := script.Exec(ctx, a.valkeyClient, keys, args).Error()
	if err != nil && !valkey.IsValkeyNil(err) {
		return fmt.Errorf("failed to update restart count '%s' and total restart count '%s' to Valkey with Lua script: %w", parsedRestartCount, parsedTotalRestartCount, err)
	}
	return nil
}

func (a *Agent) buildValkeyBroadcastChannelName() string {
	return fmt.Sprintf("%s:%s", valkeyJobSetUidKeyPreffix, a.workerJobSetUid)
}

func (a *Agent) buildValkeyRestartCountKey() string {
	return fmt.Sprintf("%s:%s:%s", valkeyJobSetUidKeyPreffix, a.workerJobSetUid, valkeyRestartCountKeySuffix)
}

func (a *Agent) buildValkeyTotalRestartCountKey() string {
	return fmt.Sprintf("%s:%s:%s", valkeyJobSetUidKeyPreffix, a.workerJobSetUid, valkeyTotalRestartCountKeySuffix)
}
