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

	"github.com/valkey-io/valkey-go"
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
	kubeClient   client.Client
	valkeyClient valkey.Client
	options      OrchestratorOptions
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

func NewOrchestrator(kubeClient client.Client, valkeyClient valkey.Client, options OrchestratorOptions) (*Orchestrator, error) {
	return &Orchestrator{
		kubeClient:   kubeClient,
		valkeyClient: valkeyClient,
		options:      options,
	}, nil
}

func (o *Orchestrator) NeedLeaderElection() bool {
	return true
}

func (o *Orchestrator) Start(ctx context.Context) error {
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
	allKeys, err := o.getAllKeysFromValkey(ctx)
	if err != nil {
		log.Error(err, "failed to get all keys from Valkey")
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
func (o *Orchestrator) getAllKeysFromValkey(ctx context.Context) ([]string, error) {
	pattern := fmt.Sprintf("%s:*", jobSetUidKeyPrefix)
	keys, err := o.valkeyClient.Do(ctx, o.valkeyClient.B().Keys().Pattern(pattern).Build()).AsStrSlice()
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
	workloadData, err := o.getWorkloadDataFromValkey(ctx, jobsetUid)
	if err != nil {
		log.Error(err, "failed to get workload data from Valkey")
		return
	}
	log.Info(fmt.Sprintf("workload data: %#v", workloadData)) // TODO: Remove after finishing function
	err = o.handlePollCases(ctx, jobsetUid, workloadData)
	if err != nil {
		log.Error(err, "failed to handle poll cases")
		return
	}
}

func (o *Orchestrator) getWorkloadDataFromValkey(ctx context.Context, jobsetUid string) (WorkloadData, error) {
	rawWorkerCount, rawTimeoutDuration, rawState, rawTotalRestartCountByWorkerId, err := o.getRawWorkloadDataFromValkey(ctx, jobsetUid)
	if err != nil {
		return WorkloadData{}, err
	}
	workloadData, err := o.parseWorkloadData(rawWorkerCount, rawTimeoutDuration, rawState, rawTotalRestartCountByWorkerId)
	if err != nil {
		return WorkloadData{}, err
	}
	return workloadData, nil
}

func (o *Orchestrator) getRawWorkloadDataFromValkey(ctx context.Context, jobsetUid string) (string, string, map[string]string, map[string]string, error) {
	script := valkey.NewLuaScript(`
	local value1 = server.call('GET', KEYS[1])
	local value2 = server.call('GET', KEYS[2])
	local value3 = server.call('HGETALL', KEYS[3])
	local value4 = server.call('HGETALL', KEYS[4])

	return { value1, value2, value3, value4 }
	`)
	keys := []string{
		o.buildValkeyWorkerCountKeyName(jobsetUid),
		o.buildValkeyTimeoutDurationKeyName(jobsetUid),
		o.buildValkeyStateKeyName(jobsetUid),
		o.buildValkeyTotalRestartCountByWorkerIdKeyName(jobsetUid),
	}
	args := []string{}
	list, err := script.Exec(ctx, o.valkeyClient, keys, args).ToArray()
	if err != nil {
		return "", "", map[string]string{}, map[string]string{}, fmt.Errorf("failed to get workload data from Valkey: %w", err)
	}
	workerCount, err := list[0].ToString()
	if err != nil {
		return "", "", map[string]string{}, map[string]string{}, fmt.Errorf("failed to extract value from restart count: %w", err)
	}
	timeoutDuration, err := list[1].ToString()
	if err != nil {
		return "", "", map[string]string{}, map[string]string{}, fmt.Errorf("failed to extract value from timeout duration: %w", err)
	}
	state, err := list[2].AsStrMap()
	if err != nil {
		return "", "", map[string]string{}, map[string]string{}, fmt.Errorf("failed to extract value from state: %w", err)
	}
	totalRestartCountByWorkerId, err := list[3].AsStrMap()
	if err != nil {
		return "", "", map[string]string{}, map[string]string{}, fmt.Errorf("failed to extract value from total restart count by worker id: %w", err)
	}
	return workerCount, timeoutDuration, state, totalRestartCountByWorkerId, nil
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

func (o *Orchestrator) buildValkeyWorkerCountKeyName(jobsetUid string) string {
	return fmt.Sprintf("%s:%s:%s", jobSetUidKeyPrefix, jobsetUid, workerCountKeySufix)
}

func (o *Orchestrator) buildValkeyTimeoutDurationKeyName(jobsetUid string) string {
	return fmt.Sprintf("%s:%s:%s", jobSetUidKeyPrefix, jobsetUid, timeoutDurationKeySufix)
}

func (o *Orchestrator) buildValkeyStateKeyName(jobsetUid string) string {
	return fmt.Sprintf("%s:%s:%s", jobSetUidKeyPrefix, jobsetUid, stateKeySufix)
}

func (o *Orchestrator) buildValkeyTotalRestartCountByWorkerIdKeyName(jobsetUid string) string {
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
	now, err := o.getCurrentTimeFromValkey(ctx)
	if err != nil {
		return err
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
	now, err := o.getCurrentTimeFromValkey(ctx)
	if err != nil {
		return err
	}
	err = o.valkeyClient.Do(ctx, o.valkeyClient.B().Hset().Key(o.buildValkeyStateKeyName(jobsetUid)).FieldValue().
		FieldValue(stateNameKeyName, stateNameRunning).
		FieldValue(stateStartTimeKeyName, now.Format(time.RFC3339)).
		FieldValue(stateDesiredTotalRestartCountKeyName, strconv.Itoa(newDesiredTotalRestartCount)).
		Build()).Error()
	if err != nil {
		return fmt.Errorf("failed to update workload state to Valkey: %w", err)
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
	now, err := o.getCurrentTimeFromValkey(ctx)
	if err != nil {
		return err
	}
	script := valkey.NewLuaScript(`
	server.call('HSET', KEYS[1], ARGV[1], ARGV[2], ARGV[3], ARGV[4], ARGV[5], ARGV[6])
	server.call('PUBLISH', ARGV[7], ARGV[8])
	`)
	keys := []string{
		o.buildValkeyStateKeyName(jobsetUid),
	}
	args := []string{
		stateNameKeyName,
		stateNameRestarting,
		stateStartTimeKeyName,
		now.Format(time.RFC3339),
		stateDesiredTotalRestartCountKeyName,
		strconv.Itoa(newDesiredTotalRestartCount),
		o.options.BroadcastChannelName,
		string(jsonData),
	}
	err = script.Exec(ctx, o.valkeyClient, keys, args).Error()
	if err != nil {
		return fmt.Errorf("failed to publish restart signal and update workload state to Valkey: %w", err)
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
	err = o.setStateToRecreatingAndRemoveWorkerDataFromValkey(ctx, jobsetUid)
	if err != nil {
		return fmt.Errorf("failed to update workload state and remove worker data from Valkey: %w", err)
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

func (o *Orchestrator) setStateToRecreatingAndRemoveWorkerDataFromValkey(ctx context.Context, jobsetUid string) error {
	now, err := o.getCurrentTimeFromValkey(ctx)
	if err != nil {
		return err
	}
	state := map[string]string{
		stateNameKeyName:                     stateNameRecreating,
		stateStartTimeKeyName:                now.Format(time.RFC3339),
		stateDesiredTotalRestartCountKeyName: "0",
	}
	_, err = o.valkeyClient.TxPipelined(ctx, func(transaction valkey.Pipeliner) error {
		o.valkeyClient.HSet(ctx, o.buildValkeyStateKeyName(jobsetUid), state).Err()
		transaction.Del(ctx, fmt.Sprintf("jobset-uid:%s:restart-count-by-worker-id", jobsetUid))
		transaction.Del(ctx, fmt.Sprintf("jobset-uid:%s:total-restart-count-by-worker-id", jobsetUid))
		return nil
	})
	if err != nil {
		return fmt.Errorf("failed to update workload state and remove worker data from Valkey: %w", err)
	}
	return nil
}

func (o *Orchestrator) getCurrentTimeFromValkey(ctx context.Context) (time.Time, error) {
	timestamps, err := o.valkeyClient.Do(ctx, o.valkeyClient.B().Time().Build()).AsStrSlice()
	if err != nil {
		return time.Time{}, fmt.Errorf("failed to get current time from Valkey: %w", err)
	}
	unixSeconds, err := strconv.ParseInt(timestamps[0], 10, 64)
	now := time.Unix(unixSeconds, 0)
	return now, nil
}
