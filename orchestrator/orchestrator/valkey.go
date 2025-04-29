package orchestrator

import (
	"context"
	"fmt"
	"strconv"
	"strings"
	"time"

	"github.com/valkey-io/valkey-go"
	"k8s.io/klog/v2"
)

const (
	valkeyJobSetUidKeyPreffix                  = "jobset-uid"
	valkeyStateKeySuffix                       = "state"
	valkeyRestartCountByWorkerIdKeySuffix      = "restart-count-by-worker-id"
	valkeyTotalRestartCountByWorkerIdKeySuffix = "total-restart-count-by-worker-id"
	valkeyStateNameField                       = "name"
	valkeyStateStartTimeField                  = "start-time"
	valkeyStateDesiredTotalRestartCountField   = "desired-total-restart-count"
	valkeyLockKey                              = "lock"
	valkeyLockDuration                         = "30"
)

type JobSetData struct {
	State                       StateData
	TotalRestartCountByWorkerId map[string]int
}

type StateData struct {
	Name                     string
	StartTime                time.Time
	DesiredTotalRestartCount int
}

type MessageData struct {
	DesiredTotalRestartCount int `json:"desiredTotalRestartCount"`
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

func (o *Orchestrator) acquireLock(ctx context.Context, podUid string) (bool, error) {
	klog.V(2).Infof("Acquiring lock. Pod UID=%s", podUid)
	script := valkey.NewLuaScript(`
	if server.call('GET', KEYS[1]) == false then
		server.call('SET', KEYS[1], ARGV[1], 'EX', ARGV[2])
		return 1
	else
		return 0
	end
	`)
	keys := []string{
		valkeyLockKey,
	}
	args := []string{
		podUid,
		valkeyLockDuration,
	}
	acquired, err := script.Exec(ctx, o.valkeyClient, keys, args).AsBool()
	if err != nil {
		return false, fmt.Errorf("failed to acquire lock: %w", err)
	}
	return acquired, nil
}

func (o *Orchestrator) renewLock(ctx context.Context, podUid string) error {
	klog.V(2).Infof("Renewing lock. Pod UID=%s", podUid)
	script := valkey.NewLuaScript(`
	if server.call("GET", KEYS[1]) == ARGV[1] then
		return server.call("EXPIRE", KEYS[1], ARGV[2])
	else
		return 0
	end
	`)
	keys := []string{
		valkeyLockKey,
	}
	args := []string{
		podUid,
		valkeyLockDuration,
	}
	err := script.Exec(ctx, o.valkeyClient, keys, args).Error()
	if err != nil {
		return fmt.Errorf("failed to release lock: %w", err)
	}
	return nil
}

func (o *Orchestrator) releaseLock(ctx context.Context, podUid string) error {
	klog.Infof("Releasing lock. Pod UID=%s", podUid)
	script := valkey.NewLuaScript(`
	if server.call("GET", KEYS[1]) == ARGV[1] then
		return server.call("DEL", KEYS[1])
	else
		return 0
	end
	`)
	keys := []string{
		valkeyLockKey,
	}
	args := []string{
		podUid,
	}
	err := script.Exec(ctx, o.valkeyClient, keys, args).Error()
	if err != nil {
		return fmt.Errorf("failed to release lock: %w", err)
	}
	return nil
}

func (o *Orchestrator) listJobSetsFromValkey(ctx context.Context) ([]string, error) {
	klog.V(2).Info("Listing JobSets from Valkey")
	keys, err := o.getStateKeysFromValkey(ctx)
	if err != nil {
		return []string{}, fmt.Errorf("failed to get state keys from Valkey: %w", err)
	}
	jobSetUids, err := o.extractJobSetUidsFromStateKeys(keys)
	if err != nil {
		return []string{}, fmt.Errorf("failed to extract JobSet UIDs from state keys: %w", err)
	}
	return jobSetUids, nil
}

// TODO: Refactor to use SCAN
func (o *Orchestrator) getStateKeysFromValkey(ctx context.Context) ([]string, error) {
	klog.V(2).Info("Getting state keys from Valkey")
	pattern := fmt.Sprintf("%s:*:%s", valkeyJobSetUidKeyPreffix, valkeyStateKeySuffix)
	keys, err := o.valkeyClient.Do(ctx, o.valkeyClient.B().Keys().Pattern(pattern).Build()).AsStrSlice()
	if err != nil {
		return []string{}, err
	}
	return keys, nil
}

func (o *Orchestrator) extractJobSetUidsFromStateKeys(keys []string) ([]string, error) {
	klog.V(2).Info("Extracting JobSet UIDs from state keys")
	jobSetUids := []string{}
	for _, key := range keys {
		parts := strings.Split(key, ":")
		jobSetUid := parts[1]
		jobSetUids = append(jobSetUids, jobSetUid)
	}
	return jobSetUids, nil
}

func (o *Orchestrator) getJobSetDataFromValkey(ctx context.Context, jobSetUid string) (JobSetData, error) {
	klog.V(2).Infof("Getting JobSet data from Valkey. JobSet UID=%s", jobSetUid)
	rawState, rawTotalRestartCountByWorkerId, err := o.getRawJobSetDataFromValkey(ctx, jobSetUid)
	if err != nil {
		return JobSetData{}, err
	}
	jobSetData, err := o.parseJobSetData(rawState, rawTotalRestartCountByWorkerId)
	if err != nil {
		return JobSetData{}, err
	}
	return jobSetData, nil
}

func (o *Orchestrator) getRawJobSetDataFromValkey(ctx context.Context, jobSetUid string) (map[string]string, map[string]string, error) {
	script := valkey.NewLuaScript(`
	local value1 = server.call('HGETALL', KEYS[1])
	local value2 = server.call('HGETALL', KEYS[2])

	return { value1, value2 }
	`)
	keys := []string{
		o.buildValkeyStateKey(jobSetUid),
		o.buildValkeyTotalRestartCountByWorkerIdKey(jobSetUid),
	}
	args := []string{}
	list, err := script.Exec(ctx, o.valkeyClient, keys, args).ToArray()
	if err != nil {
		return map[string]string{}, map[string]string{}, fmt.Errorf("failed to get JobSet data from Valkey: %w", err)
	}
	state, err := list[0].AsStrMap()
	if err != nil {
		return map[string]string{}, map[string]string{}, fmt.Errorf("failed to extract value from state: %w", err)
	}
	totalRestartCountByWorkerId, err := list[1].AsStrMap()
	if err != nil {
		return map[string]string{}, map[string]string{}, fmt.Errorf("failed to extract value from total restart count by worker id: %w", err)
	}
	return state, totalRestartCountByWorkerId, nil
}

func (o *Orchestrator) parseJobSetData(rawState map[string]string, rawTotalRestartCountByWorkerId map[string]string) (JobSetData, error) {
	startTime, err := time.Parse(time.RFC3339, rawState[valkeyStateStartTimeField])
	if err != nil {
		return JobSetData{}, fmt.Errorf("failed to parse start time from state: %w", err)
	}
	desiredTotalRestartCount, err := strconv.Atoi(rawState[valkeyStateDesiredTotalRestartCountField])
	if err != nil {
		return JobSetData{}, fmt.Errorf("failed to parse desired total restart count from state: %w", err)
	}
	state := StateData{
		Name:                     rawState[valkeyStateNameField],
		StartTime:                startTime,
		DesiredTotalRestartCount: desiredTotalRestartCount,
	}
	totalRestartCountByWorkerId := map[string]int{}
	for workerId, rawTotalRestartCount := range rawTotalRestartCountByWorkerId {
		totalRestartCount, err := strconv.Atoi(rawTotalRestartCount)
		if err != nil {
			return JobSetData{}, fmt.Errorf("failed to parse total restart count for worker id %s: %w", workerId, err)
		}
		totalRestartCountByWorkerId[workerId] = totalRestartCount
	}
	jobSetData := JobSetData{
		State:                       state,
		TotalRestartCountByWorkerId: totalRestartCountByWorkerId,
	}
	return jobSetData, nil
}

func (o *Orchestrator) updateJobSetStateToValkey(ctx context.Context, jobSetUid string, name string, startTime time.Time, desiredTotalRestartCount int) error {
	script := valkey.NewLuaScript(`
	server.call('HSET', KEYS[1], ARGV[1], ARGV[2], ARGV[3], ARGV[4], ARGV[5], ARGV[6])
	`)
	keys := []string{
		o.buildValkeyStateKey(jobSetUid),
	}
	args := []string{
		valkeyStateNameField,
		name,
		valkeyStateStartTimeField,
		startTime.Format(time.RFC3339),
		valkeyStateDesiredTotalRestartCountField,
		strconv.Itoa(desiredTotalRestartCount),
	}
	err := script.Exec(ctx, o.valkeyClient, keys, args).Error()
	if err != nil && !valkey.IsValkeyNil(err) {
		return fmt.Errorf("failed to update JobSet state to Valkey with Lua script: %w", err)
	}
	return nil
}

func (o *Orchestrator) updateJobSetStateAndPublishMessageToValkey(ctx context.Context, jobSetUid string, name string, startTime time.Time, desiredTotalRestartCount int, jsonData []byte) error {
	script := valkey.NewLuaScript(`
	server.call('HSET', KEYS[1], ARGV[1], ARGV[2], ARGV[3], ARGV[4], ARGV[5], ARGV[6])
	server.call('PUBLISH', ARGV[7], ARGV[8])
	`)
	keys := []string{
		o.buildValkeyStateKey(jobSetUid),
	}
	args := []string{
		valkeyStateNameField,
		stateNameRestarting,
		valkeyStateStartTimeField,
		startTime.Format(time.RFC3339),
		valkeyStateDesiredTotalRestartCountField,
		strconv.Itoa(desiredTotalRestartCount),
		o.buildValkeyBroadcastChannelName(jobSetUid),
		string(jsonData),
	}
	err := script.Exec(ctx, o.valkeyClient, keys, args).Error()
	if err != nil && !valkey.IsValkeyNil(err) {
		return fmt.Errorf("failed to update JobSet state and publish message to Valkey with Lua script: %w", err)
	}
	return nil
}

func (o *Orchestrator) updateJobSetStateAndDeleteCountsToValkey(ctx context.Context, jobSetUid string, name string, startTime time.Time, desiredTotalRestartCount int) error {
	script := valkey.NewLuaScript(`
	redis.call('HSET', KEYS[1], ARGV[1], ARGV[2], ARGV[3], ARGV[4], ARGV[5], ARGV[6])
	redis.call('DEL', KEYS[2])
	redis.call('DEL', KEYS[3])
	`)
	keys := []string{
		o.buildValkeyStateKey(jobSetUid),
		o.buildValkeyRestartCountByWorkerIdKey(jobSetUid),
		o.buildValkeyTotalRestartCountByWorkerIdKey(jobSetUid),
	}
	args := []string{
		valkeyStateNameField,
		name,
		valkeyStateStartTimeField,
		startTime.Format(time.RFC3339),
		valkeyStateDesiredTotalRestartCountField,
		strconv.Itoa(desiredTotalRestartCount),
	}
	err := script.Exec(ctx, o.valkeyClient, keys, args).Error()
	if err != nil && !valkey.IsValkeyNil(err) {
		return fmt.Errorf("failed to update JobSet state and delete counts to Valkey with Lua script: %w", err)
	}
	return nil
}

func (o *Orchestrator) deleteJobSetStateFromValkey(ctx context.Context, jobSetUid string) {
	script := valkey.NewLuaScript(`
	server.call('DEL', KEYS[1])
	server.call('DEL', KEYS[2])
	server.call('DEL', KEYS[3])
	`)
	keys := []string{
		o.buildValkeyStateKey(jobSetUid),
		o.buildValkeyRestartCountByWorkerIdKey(jobSetUid),
		o.buildValkeyTotalRestartCountByWorkerIdKey(jobSetUid),
	}
	args := []string{}
	err := script.Exec(ctx, o.valkeyClient, keys, args).Error()
	if err != nil && !valkey.IsValkeyNil(err) {
		klog.Errorf("Failed to remove JobSet from Valkey. JobSet UID=%s: %v", jobSetUid, err)
	}
}

func (o *Orchestrator) getCurrentTimeFromValkey(ctx context.Context) (time.Time, error) {
	timestamps, err := o.valkeyClient.Do(ctx, o.valkeyClient.B().Time().Build()).AsStrSlice()
	if err != nil {
		return time.Time{}, fmt.Errorf("failed to get current time from Valkey: %w", err)
	}
	unixSeconds, err := strconv.ParseInt(timestamps[0], 10, 64)
	if err != nil {
		return time.Time{}, fmt.Errorf("failed to parse current time from Valkey: %w", err)
	}
	now := time.Unix(unixSeconds, 0)
	return now, nil
}

func (o *Orchestrator) buildValkeyBroadcastChannelName(jobSetUid string) string {
	return fmt.Sprintf("%s:%s", valkeyJobSetUidKeyPreffix, jobSetUid)
}

func (o *Orchestrator) buildValkeyStateKey(jobSetUid string) string {
	return fmt.Sprintf("%s:%s:%s", valkeyJobSetUidKeyPreffix, jobSetUid, valkeyStateKeySuffix)
}

func (o *Orchestrator) buildValkeyRestartCountByWorkerIdKey(jobSetUid string) string {
	return fmt.Sprintf("%s:%s:%s", valkeyJobSetUidKeyPreffix, jobSetUid, valkeyRestartCountByWorkerIdKeySuffix)
}

func (o *Orchestrator) buildValkeyTotalRestartCountByWorkerIdKey(jobSetUid string) string {
	return fmt.Sprintf("%s:%s:%s", valkeyJobSetUidKeyPreffix, jobSetUid, valkeyTotalRestartCountByWorkerIdKeySuffix)
}
