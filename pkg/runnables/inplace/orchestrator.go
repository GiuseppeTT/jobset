package inplace

import (
	"context"
	"encoding/json"
	"fmt"
	"strconv"
	"strings"
	"time"

	batchv1 "k8s.io/api/batch/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"

	"github.com/redis/go-redis/v9"
)

// Move constants to jobset_types.go
const (
	jobSetUidKeyPrefix                   = "jobset-uid"
	workerCountKeySufix                  = "worker-count"
	timeoutDurationKeySufix              = "timeout-duration"
	stateKeySufix                        = "state"
	totalRestartCountByWorkerIdKeySufix  = "total-restart-count-by-worker-id"
	stateNameKeyName                     = "name"
	stateStartTimeKeyName                = "start-time"
	stateDesiredTotalRestartCountKeyName = "desired-total-restart-count"
	stateNameCreating                    = "creating"
	stateNameRunning                     = "running"
	stateNameRestarting                  = "restarting"
	stateNameRecreating                  = "recreating"
)

type Orchestrator struct {
	kubeClient  client.Client
	redisClient *redis.Client
	options     OrchestratorOptions
}

type OrchestratorOptions struct {
	PollingInterval      time.Duration
	BroadcastChannelName string
}

type WorkloadData struct {
	WorkerCount                 int
	TimeoutDuration             time.Duration
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

func NewOrchestrator(kubeClient client.Client, redisClient *redis.Client, options OrchestratorOptions) (*Orchestrator, error) {
	return &Orchestrator{
		kubeClient:  kubeClient,
		redisClient: redisClient,
		options:     options,
	}, nil
}

func (o *Orchestrator) NeedLeaderElection() bool {
	return true
}

func (o *Orchestrator) Start(ctx context.Context) error {
	defer o.redisClient.Close()
	ticker := time.NewTicker(o.options.PollingInterval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return nil
		case <-ticker.C:
			o.pollAllWorkloads(ctx)
		}
	}
}

func (o *Orchestrator) pollAllWorkloads(ctx context.Context) {
	log := ctrl.LoggerFrom(ctx).WithName("orchestrator")
	log.Info("polling all workloads")
	allKeys, err := o.getAllKeysFromRedis(ctx)
	if err != nil {
		log.Error(err, "failed to get all keys from Redis")
		return
	}
	jobsetUids, err := o.extractJobsetUidsFromKeys(allKeys)
	if err != nil {
		log.Error(err, "failed to extract JobSet UIDs from keys")
		return
	}
	for _, jobsetUid := range jobsetUids {
		o.pollWorkload(ctx, jobsetUid)
	}
}

// TODO: Refactor to use SCAN
func (o *Orchestrator) getAllKeysFromRedis(ctx context.Context) ([]string, error) {
	pattern := fmt.Sprintf("%s:*", jobSetUidKeyPrefix)
	keys, err := o.redisClient.Keys(ctx, pattern).Result()
	if err != nil {
		return []string{}, err
	}
	return keys, nil
}

func (o *Orchestrator) extractJobsetUidsFromKeys(keys []string) ([]string, error) {
	jobsetUidSet := map[string]struct{}{}
	for _, key := range keys {
		parts := strings.Split(key, ":")
		jobsetUid := parts[1]
		jobsetUidSet[jobsetUid] = struct{}{}
	}
	jobsetUids := []string{}
	for jobsetUid := range jobsetUidSet {
		jobsetUids = append(jobsetUids, jobsetUid)
	}
	return jobsetUids, nil
}

// TODO: Finish
func (o *Orchestrator) pollWorkload(ctx context.Context, jobsetUid string) {
	log := ctrl.LoggerFrom(ctx).WithName("orchestrator").WithValues("jobset-uid", jobsetUid)
	log.Info("polling workload")
	workloadData, err := o.getWorkloadDataFromRedis(ctx, jobsetUid)
	if err != nil {
		log.Error(err, "failed to get workload data from Redis")
		return
	}
	log.Info(fmt.Sprintf("workload data: %#v", workloadData)) // TODO: Remove after finishing function
	err = o.handlePollCases(ctx, jobsetUid, workloadData)
	if err != nil {
		log.Error(err, "failed to handle poll cases")
		return
	}
}

func (o *Orchestrator) getWorkloadDataFromRedis(ctx context.Context, jobsetUid string) (WorkloadData, error) {
	rawWorkerCount, rawTimeoutDuration, rawState, rawTotalRestartCountByWorkerId, err := o.getRawWorkloadDataFromRedis(ctx, jobsetUid)
	if err != nil {
		return WorkloadData{}, err
	}
	workloadData, err := o.parseWorkloadData(rawWorkerCount, rawTimeoutDuration, rawState, rawTotalRestartCountByWorkerId)
	if err != nil {
		return WorkloadData{}, err
	}
	return workloadData, nil
}

func (o *Orchestrator) getRawWorkloadDataFromRedis(ctx context.Context, jobsetUid string) (string, string, map[string]string, map[string]string, error) {
	var workerCount *redis.StringCmd
	var timeoutDuration *redis.StringCmd
	var state *redis.MapStringStringCmd
	var totalRestartCountByWorkerId *redis.MapStringStringCmd
	_, err := o.redisClient.TxPipelined(ctx, func(transaction redis.Pipeliner) error {
		workerCount = transaction.Get(ctx, o.buildRedisWorkerCountKeyName(jobsetUid))
		timeoutDuration = transaction.Get(ctx, o.buildRedisTimeoutDurationKeyName(jobsetUid))
		state = transaction.HGetAll(ctx, o.buildRedisStateKeyName(jobsetUid))
		totalRestartCountByWorkerId = transaction.HGetAll(ctx, o.buildRedisTotalRestartCountByWorkerIdKeyName(jobsetUid))
		return nil
	})
	if err != nil {
		return "", "", map[string]string{}, map[string]string{}, fmt.Errorf("failed to get workload data in transaction from Redis: %w", err)
	}
	return workerCount.Val(), timeoutDuration.Val(), state.Val(), totalRestartCountByWorkerId.Val(), nil
}

func (o *Orchestrator) parseWorkloadData(rawWorkerCount string, rawTimeoutDuration string, rawState map[string]string, rawTotalRestartCountByWorkerId map[string]string) (WorkloadData, error) {
	workerCount, err := strconv.Atoi(rawWorkerCount)
	if err != nil {
		return WorkloadData{}, fmt.Errorf("failed to parse worker count: %w", err)
	}
	timeoutDuration, err := strconv.Atoi(rawTimeoutDuration)
	if err != nil {
		return WorkloadData{}, fmt.Errorf("failed to parse timeout duration: %w", err)
	}
	startTime, err := time.Parse(time.RFC3339, rawState[stateStartTimeKeyName])
	if err != nil {
		return WorkloadData{}, fmt.Errorf("failed to parse start time from state: %w", err)
	}
	desiredTotalRestartCount, err := strconv.Atoi(rawState[stateDesiredTotalRestartCountKeyName])
	if err != nil {
		return WorkloadData{}, fmt.Errorf("failed to parse desired total restart count from state: %w", err)
	}
	state := StateData{
		Name:                     rawState[stateNameKeyName],
		StartTime:                startTime,
		DesiredTotalRestartCount: desiredTotalRestartCount,
	}
	totalRestartCountByWorkerId := map[string]int{}
	for workerId, rawTotalRestartCount := range rawTotalRestartCountByWorkerId {
		totalRestartCount, err := strconv.Atoi(rawTotalRestartCount)
		if err != nil {
			return WorkloadData{}, fmt.Errorf("failed to parse total restart count for worker id %s: %w", workerId, err)
		}
		totalRestartCountByWorkerId[workerId] = totalRestartCount
	}
	workloadData := WorkloadData{
		WorkerCount:                 workerCount,
		TimeoutDuration:             time.Duration(timeoutDuration) * time.Second,
		State:                       state,
		TotalRestartCountByWorkerId: totalRestartCountByWorkerId,
	}
	return workloadData, nil
}

func (o *Orchestrator) buildRedisWorkerCountKeyName(jobsetUid string) string {
	return fmt.Sprintf("%s:%s:%s", jobSetUidKeyPrefix, jobsetUid, workerCountKeySufix)
}

func (o *Orchestrator) buildRedisTimeoutDurationKeyName(jobsetUid string) string {
	return fmt.Sprintf("%s:%s:%s", jobSetUidKeyPrefix, jobsetUid, timeoutDurationKeySufix)
}

func (o *Orchestrator) buildRedisStateKeyName(jobsetUid string) string {
	return fmt.Sprintf("%s:%s:%s", jobSetUidKeyPrefix, jobsetUid, stateKeySufix)
}

func (o *Orchestrator) buildRedisTotalRestartCountByWorkerIdKeyName(jobsetUid string) string {
	return fmt.Sprintf("%s:%s:%s", jobSetUidKeyPrefix, jobsetUid, totalRestartCountByWorkerIdKeySufix)
}

// TODO: Handle request for full recreation
func (o *Orchestrator) handlePollCases(ctx context.Context, jobsetUid string, workloadData WorkloadData) error {
	log := ctrl.LoggerFrom(ctx).WithName("orchestrator").WithValues("jobset-uid", jobsetUid)
	log.Info("polling workload") // TODO: Remove after finishing function
	if workloadData.State.Name == stateNameCreating {
		return o.handleWorkloadCreatingCase(ctx, jobsetUid, workloadData)
	}
	if workloadData.State.Name == stateNameRunning {
		return o.handleWorkloadRunningCase(ctx, jobsetUid, workloadData)
	}
	if workloadData.State.Name == stateNameRestarting {
		return o.handleWorkloadRestartingCase(ctx, jobsetUid, workloadData)
	}
	if workloadData.State.Name == stateNameRecreating {
		return o.handleWorkloadRecreatingCase(ctx, jobsetUid, workloadData)
	}
	return fmt.Errorf("unknown workload state: %s", workloadData.State.Name)
}

func (o *Orchestrator) handleWorkloadCreatingCase(ctx context.Context, jobsetUid string, workloadData WorkloadData) error {
	log := ctrl.LoggerFrom(ctx).WithName("orchestrator").WithValues("jobset-uid", jobsetUid)
	log.Info("handle workload creating case")
	matchingCount := 0
	for workerId, totalRestartCount := range workloadData.TotalRestartCountByWorkerId {
		if totalRestartCount == 0 {
			matchingCount += 1
			continue
		}
		if totalRestartCount < 0 {
			log.Info(fmt.Sprintf("Detected request for full recreation. Total restart count %d for worker %s. Starting full recreation", totalRestartCount, workerId))
		} else {
			log.Error(nil, fmt.Sprintf("invalid total restart count %d for worker %s. Starting full recreation", totalRestartCount, workerId))
		}
		return o.startFullRecreation(ctx, jobsetUid)
	}
	if matchingCount == workloadData.WorkerCount {
		return o.startFullRunning(ctx, jobsetUid, workloadData.State.DesiredTotalRestartCount)
	}
	return nil
}

func (o *Orchestrator) handleWorkloadRunningCase(ctx context.Context, jobsetUid string, workloadData WorkloadData) error {
	log := ctrl.LoggerFrom(ctx).WithName("orchestrator").WithValues("jobset-uid", jobsetUid)
	log.Info("handle workload running case")
	requiresRestart := false
	for workerId, totalRestartCount := range workloadData.TotalRestartCountByWorkerId {
		if totalRestartCount == workloadData.State.DesiredTotalRestartCount {
			continue
		}
		if totalRestartCount == workloadData.State.DesiredTotalRestartCount+1 {
			requiresRestart = true
			continue
		}
		if totalRestartCount < 0 {
			log.Info(fmt.Sprintf("Detected request for full recreation. Total restart count %d for worker %s. Starting full recreation", totalRestartCount, workerId))
		} else {
			log.Error(nil, fmt.Sprintf("invalid total restart count %d for worker %s. Starting full recreation", totalRestartCount, workerId))
		}
		return o.startFullRecreation(ctx, jobsetUid)
	}
	if requiresRestart {
		return o.startFullRestart(ctx, jobsetUid, workloadData.State.DesiredTotalRestartCount+1)
	}
	return nil
}

func (o *Orchestrator) handleWorkloadRestartingCase(ctx context.Context, jobsetUid string, workloadData WorkloadData) error {
	log := ctrl.LoggerFrom(ctx).WithName("orchestrator").WithValues("jobset-uid", jobsetUid)
	log.Info("handle workload restarting case")
	now, err := o.redisClient.Time(ctx).Result()
	if err != nil {
		return fmt.Errorf("failed to get current time from Redis: %w", err)
	}
	if now.After(workloadData.State.StartTime.Add(workloadData.TimeoutDuration)) {
		log.Error(nil, "full restart timed out. Starting full recreation")
		return o.startFullRecreation(ctx, jobsetUid)
	}
	matchingCount := 0
	for workerId, totalRestartCount := range workloadData.TotalRestartCountByWorkerId {
		if totalRestartCount == workloadData.State.DesiredTotalRestartCount-1 {
			continue
		}
		if totalRestartCount == workloadData.State.DesiredTotalRestartCount {
			matchingCount += 1
			continue
		}
		if totalRestartCount < 0 {
			log.Info(fmt.Sprintf("Detected request for full recreation. Total restart count %d for worker %s. Starting full recreation", totalRestartCount, workerId))
		} else {
			log.Error(nil, fmt.Sprintf("invalid total restart count %d for worker %s. Starting full recreation", totalRestartCount, workerId))
		}
		return o.startFullRecreation(ctx, jobsetUid)
	}
	if matchingCount == workloadData.WorkerCount {
		return o.startFullRunning(ctx, jobsetUid, workloadData.State.DesiredTotalRestartCount)
	}
	return nil
}

func (o *Orchestrator) handleWorkloadRecreatingCase(ctx context.Context, jobsetUid string, workloadData WorkloadData) error {
	log := ctrl.LoggerFrom(ctx).WithName("orchestrator").WithValues("jobset-uid", jobsetUid)
	log.Info("handle workload recreating case")
	matchingCount := 0
	for workerId, totalRestartCount := range workloadData.TotalRestartCountByWorkerId {
		if totalRestartCount == 0 {
			matchingCount += 1
			continue
		}
		if totalRestartCount < 0 {
			log.Info(fmt.Sprintf("Detected request for full recreation. Total restart count %d for worker %s. Starting full recreation", totalRestartCount, workerId))
		} else {
			log.Error(nil, fmt.Sprintf("invalid total restart count %d for worker %s. Starting full recreation", totalRestartCount, workerId))
		}
		return o.startFullRecreation(ctx, jobsetUid)
	}
	if matchingCount == workloadData.WorkerCount {
		return o.startFullRunning(ctx, jobsetUid, workloadData.State.DesiredTotalRestartCount)
	}
	return nil
}

func (o *Orchestrator) startFullRunning(ctx context.Context, jobsetUid string, newDesiredTotalRestartCount int) error {
	log := ctrl.LoggerFrom(ctx).WithName("orchestrator").WithValues("jobset-uid", jobsetUid)
	log.Info("start full running")
	now, err := o.redisClient.Time(ctx).Result()
	if err != nil {
		return fmt.Errorf("failed to get current time from Redis: %w", err)
	}
	state := map[string]string{
		stateNameKeyName:                     stateNameRunning,
		stateStartTimeKeyName:                now.Format(time.RFC3339),
		stateDesiredTotalRestartCountKeyName: strconv.Itoa(newDesiredTotalRestartCount),
	}
	err = o.redisClient.HSet(ctx, o.buildRedisStateKeyName(jobsetUid), state).Err()
	if err != nil {
		return fmt.Errorf("failed to update workload state to Redis: %w", err)
	}
	return nil
}

func (o *Orchestrator) startFullRestart(ctx context.Context, jobsetUid string, newDesiredTotalRestartCount int) error {
	log := ctrl.LoggerFrom(ctx).WithName("orchestrator").WithValues("jobset-uid", jobsetUid)
	log.Info("start full restart")
	data := MessageData{
		DesiredTotalRestartCount: newDesiredTotalRestartCount,
	}
	jsonData, err := json.Marshal(data)
	if err != nil {
		return fmt.Errorf("failed to marshall message data to JSON: %w", err)
	}
	now, err := o.redisClient.Time(ctx).Result()
	if err != nil {
		return fmt.Errorf("failed to get current time from Redis: %w", err)
	}
	state := map[string]string{
		stateNameKeyName:                     stateNameRestarting,
		stateStartTimeKeyName:                now.Format(time.RFC3339),
		stateDesiredTotalRestartCountKeyName: strconv.Itoa(newDesiredTotalRestartCount),
	}
	_, err = o.redisClient.TxPipelined(ctx, func(transaction redis.Pipeliner) error {
		transaction.Publish(ctx, o.options.BroadcastChannelName, string(jsonData))
		transaction.HSet(ctx, o.buildRedisStateKeyName(jobsetUid), state)
		return nil
	})
	if err != nil {
		return fmt.Errorf("failed to publish restart signal and update workload state in transaction to Redis: %w", err)
	}
	return nil
}

func (o *Orchestrator) startFullRecreation(ctx context.Context, jobsetUid string) error {
	log := ctrl.LoggerFrom(ctx).WithName("orchestrator").WithValues("jobset-uid", jobsetUid)
	log.Info("start full recreation")
	err := o.deleteChildJobs(ctx, jobsetUid)
	if err != nil {
		return fmt.Errorf("failed to delete child jobs: %w", err)
	}
	err = o.setStateToRecreatingAndRemoveWorkerDataFromRedis(ctx, jobsetUid)
	if err != nil {
		return fmt.Errorf("failed to update workload state and remove worker data from Redis: %w", err)
	}
	return nil
}

func (o *Orchestrator) deleteChildJobs(ctx context.Context, jobsetUid string) error {
	log := ctrl.LoggerFrom(ctx).WithName("orchestrator").WithValues("jobset-uid", jobsetUid)
	childJobs, err := o.listChildJobs(ctx, jobsetUid)
	if err != nil {
		return fmt.Errorf("failed to list child jobs: %w", err)
	}
	for _, childJob := range childJobs.Items {
		foregroundPolicy := metav1.DeletePropagationForeground
		err = o.kubeClient.Delete(ctx, &childJob, &client.DeleteOptions{PropagationPolicy: &foregroundPolicy})
		if err != nil {
			log.Error(err, fmt.Sprintf("failed to delete child job: %q", childJob.Name))
		}
	}
	return nil
}

func (o *Orchestrator) listChildJobs(ctx context.Context, jobsetUid string) (batchv1.JobList, error) {
	var childJobs batchv1.JobList
	err := o.kubeClient.List(ctx, &childJobs, client.MatchingLabels{"jobset.sigs.k8s.io/jobset-uid": jobsetUid})
	if err != nil {
		return batchv1.JobList{}, fmt.Errorf("failed to list jobs: %w", err)
	}
	return childJobs, nil
}

func (o *Orchestrator) setStateToRecreatingAndRemoveWorkerDataFromRedis(ctx context.Context, jobsetUid string) error {
	now, err := o.redisClient.Time(ctx).Result()
	if err != nil {
		return fmt.Errorf("failed to get current time from Redis: %w", err)
	}
	state := map[string]string{
		stateNameKeyName:                     stateNameRecreating,
		stateStartTimeKeyName:                now.Format(time.RFC3339),
		stateDesiredTotalRestartCountKeyName: "0",
	}
	_, err = o.redisClient.TxPipelined(ctx, func(transaction redis.Pipeliner) error {
		o.redisClient.HSet(ctx, o.buildRedisStateKeyName(jobsetUid), state).Err()
		transaction.Del(ctx, fmt.Sprintf("jobset-uid:%s:restart-count-by-worker-id", jobsetUid))
		transaction.Del(ctx, fmt.Sprintf("jobset-uid:%s:total-restart-count-by-worker-id", jobsetUid))
		return nil
	})
	if err != nil {
		return fmt.Errorf("failed to update workload state and remove worker data from Redis: %w", err)
	}
	return nil
}
