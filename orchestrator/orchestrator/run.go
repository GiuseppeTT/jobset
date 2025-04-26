package orchestrator

import (
	"context"
	"encoding/json"
	"fmt"
	"strconv"
	"strings"
	"time"

	"github.com/valkey-io/valkey-go"
	batchv1 "k8s.io/api/batch/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/klog/v2"
	jobset "sigs.k8s.io/jobset/api/jobset/v1alpha2"
)

type WorkloadData struct {
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

func (o *Orchestrator) Run(ctx context.Context) error {
	klog.Info("Running orchestrator")
	ticker := time.NewTicker(o.valkeyPollingInterval)
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
	klog.V(2).Info("Polling all workloads")
	newjobSetUids, currentjobSetUids, oldjobSetUids, jobSetByUid, err := o.listJobSetObjects(ctx)
	if err != nil {
		klog.Errorf("Failed to list JobSet objects: %v", err)
		return
	}
	klog.Infof("New JobSet UIDs: %#v", newjobSetUids)
	klog.Infof("Current JobSet UIDs: %#v", currentjobSetUids)
	klog.Infof("Old JobSet UIDs: %#v", oldjobSetUids)
	for _, jobSetUid := range newjobSetUids {
		o.addWorkloadToValkey(ctx, jobSetUid)
	}
	for _, jobSetUid := range oldjobSetUids {
		o.removeWorkloadFromValkey(ctx, jobSetUid)
	}
	for _, jobSetUid := range currentjobSetUids {
		jobSet := jobSetByUid[jobSetUid]
		o.pollWorkload(ctx, jobSetUid, jobSet)
	}
}

func (o *Orchestrator) listJobSetObjects(ctx context.Context) ([]string, []string, []string, map[string]jobset.JobSet, error) {
	klog.V(2).Info("Listing JobSet objects")
	kubejobSetUids, jobSetByUid, err := o.listKubejobSetUids(ctx)
	if err != nil {
		return []string{}, []string{}, []string{}, map[string]jobset.JobSet{}, fmt.Errorf("failed to list JobSet objects from k8s: %w", err)
	}
	valkeyjobSetUids, err := o.listValkeyJobSetUids(ctx)
	if err != nil {
		return []string{}, []string{}, []string{}, map[string]jobset.JobSet{}, fmt.Errorf("failed to list JobSet objects from Valkey: %w", err)
	}
	newjobSetUids, currentjobSetUids, oldjobSetUids := partitionSets(kubejobSetUids, valkeyjobSetUids)
	return newjobSetUids, currentjobSetUids, oldjobSetUids, jobSetByUid, nil
}

// TODO: Filter JobSets that have the in-place-pod-restart annotation
func (o *Orchestrator) listKubejobSetUids(ctx context.Context) ([]string, map[string]jobset.JobSet, error) {
	klog.V(2).Info("Listing JobSet objects from k8s")
	jobSetList, err := o.jobsetClient.JobsetV1alpha2().JobSets("").List(ctx, metav1.ListOptions{})
	if err != nil {
		return []string{}, map[string]jobset.JobSet{}, fmt.Errorf("failed to list JobSet objects: %w", err)
	}
	jobSetUids := []string{}
	for _, jobSet := range jobSetList.Items {
		_, containsInPlaceAnnotation := jobSet.Annotations["jobset.sigs.k8s.io/in-place-pod-restart-timeout"]
		if containsInPlaceAnnotation {
			jobSetUids = append(jobSetUids, string(jobSet.UID))
		}
	}
	jobSetByUid := map[string]jobset.JobSet{}
	for _, js := range jobSetList.Items {
		jobSetByUid[string(js.UID)] = js
	}
	return jobSetUids, jobSetByUid, nil
}

func (o *Orchestrator) listValkeyJobSetUids(ctx context.Context) ([]string, error) {
	klog.V(2).Info("Listing JobSet objects from Valkey")
	keys, err := o.getAllKeysFromValkey(ctx)
	if err != nil {
		return []string{}, fmt.Errorf("failed to get all keys from Valkey: %w", err)
	}
	jobSetUids, err := o.extractjobSetUidsFromValkeyKeys(keys)
	if err != nil {
		return []string{}, fmt.Errorf("failed to extract JobSet UIDs from Valkey keys: %w", err)
	}
	return jobSetUids, nil
}

// TODO: Refactor to use SCAN
func (o *Orchestrator) getAllKeysFromValkey(ctx context.Context) ([]string, error) {
	klog.V(2).Info("Getting all keys from Valkey")
	pattern := fmt.Sprintf("%s:*:%s", jobSetUidKeyPreffix, stateKeySuffix)
	keys, err := o.valkeyClient.Do(ctx, o.valkeyClient.B().Keys().Pattern(pattern).Build()).AsStrSlice()
	if err != nil {
		return []string{}, err
	}
	return keys, nil
}

func (o *Orchestrator) extractjobSetUidsFromValkeyKeys(keys []string) ([]string, error) {
	klog.V(2).Info("Extracting JobSet UIDs from Valkey keys")
	jobSetUidSet := map[string]struct{}{}
	for _, key := range keys {
		parts := strings.Split(key, ":")
		jobSetUid := parts[1]
		jobSetUidSet[jobSetUid] = struct{}{}
	}
	jobSetUids := []string{}
	for jobSetUid := range jobSetUidSet {
		jobSetUids = append(jobSetUids, jobSetUid)
	}
	return jobSetUids, nil
}

func partitionSets(a []string, b []string) ([]string, []string, []string) {
	klog.V(2).Info("Partitioning sets")
	setA := make(map[string]struct{}, len(a))
	for _, item := range a {
		setA[item] = struct{}{}
	}
	setB := make(map[string]struct{}, len(b))
	for _, item := range b {
		setB[item] = struct{}{}
	}
	onlyInA := []string{}
	intersection := []string{}
	onlyInB := []string{}
	for item := range setA {
		if _, foundInB := setB[item]; !foundInB {
			onlyInA = append(onlyInA, item)
		}
	}
	for item := range setB {
		if _, foundInA := setA[item]; !foundInA {
			onlyInB = append(onlyInB, item)
		}
	}
	for item := range setA {
		if _, foundInB := setB[item]; foundInB {
			intersection = append(intersection, item)
		}
	}
	return onlyInA, intersection, onlyInB
}

func (o *Orchestrator) addWorkloadToValkey(ctx context.Context, jobSetUid string) {
	klog.V(2).Infof("Adding workload to Valkey. jobSetUid=%s", jobSetUid)
	now, err := o.getCurrentTimeFromValkey(ctx)
	if err != nil {
		klog.Errorf("Failed to get current time from Valkey. jobSetUid=%s: %v", jobSetUid, err)
		return
	}
	err = o.valkeyClient.Do(ctx, o.valkeyClient.B().Hset().Key(o.buildValkeyStateKeyName(jobSetUid)).FieldValue().
		FieldValue(stateNameFieldName, stateNameCreating).
		FieldValue(stateStartTimeFieldName, now.Format(time.RFC3339)).
		FieldValue(stateDesiredTotalRestartCountFieldName, strconv.Itoa(0)).
		Build()).Error()
	if err != nil {
		klog.Errorf("Failed to add workload to Valkey. jobSetUid=%s: %v", jobSetUid, err)
		return
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

func (o *Orchestrator) removeWorkloadFromValkey(ctx context.Context, jobSetUid string) {
	script := valkey.NewLuaScript(`
	server.call('DEL', KEYS[1])
	server.call('DEL', KEYS[2])
	server.call('DEL', KEYS[3])
	`)
	keys := []string{
		o.buildValkeyStateKeyName(jobSetUid),
		o.buildValkeyRestartCountByWorkerIdKeyName(jobSetUid),
		o.buildValkeyTotalRestartCountByWorkerIdKeyName(jobSetUid),
	}
	args := []string{}
	err := script.Exec(ctx, o.valkeyClient, keys, args).Error()
	if err != nil {
		klog.Errorf("Failed to remove workload from Valkey. jobSetUid=%s: %v", jobSetUid, err)
	}
}

func (o *Orchestrator) pollWorkload(ctx context.Context, jobSetUid string, jobSet jobset.JobSet) {
	klog.Infof("Polling workload. jobSetUid=%s", jobSetUid)
	workloadData, err := o.getWorkloadDataFromValkey(ctx, jobSetUid)
	if err != nil {
		klog.Errorf("Failed to get workload data from Valkey: %v", err)
		return
	}
	klog.Infof("Workload data: %#v", workloadData) // TODO: Remove after finishing function
	err = o.handlePollCases(ctx, jobSetUid, jobSet, workloadData)
	if err != nil {
		klog.Errorf("failed to handle poll cases: %v", err)
		return
	}
}

func (o *Orchestrator) getWorkloadDataFromValkey(ctx context.Context, jobSetUid string) (WorkloadData, error) {
	rawState, rawTotalRestartCountByWorkerId, err := o.getRawWorkloadDataFromValkey(ctx, jobSetUid)
	if err != nil {
		return WorkloadData{}, err
	}
	workloadData, err := o.parseWorkloadData(rawState, rawTotalRestartCountByWorkerId)
	if err != nil {
		return WorkloadData{}, err
	}
	return workloadData, nil
}

func (o *Orchestrator) getRawWorkloadDataFromValkey(ctx context.Context, jobSetUid string) (map[string]string, map[string]string, error) {
	script := valkey.NewLuaScript(`
	local value1 = server.call('HGETALL', KEYS[1])
	local value2 = server.call('HGETALL', KEYS[2])

	return { value1, value2 }
	`)
	keys := []string{
		o.buildValkeyStateKeyName(jobSetUid),
		o.buildValkeyTotalRestartCountByWorkerIdKeyName(jobSetUid),
	}
	args := []string{}
	list, err := script.Exec(ctx, o.valkeyClient, keys, args).ToArray()
	if err != nil {
		return map[string]string{}, map[string]string{}, fmt.Errorf("failed to get workload data from Valkey: %w", err)
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

func (o *Orchestrator) parseWorkloadData(rawState map[string]string, rawTotalRestartCountByWorkerId map[string]string) (WorkloadData, error) {
	startTime, err := time.Parse(time.RFC3339, rawState[stateStartTimeFieldName])
	if err != nil {
		return WorkloadData{}, fmt.Errorf("failed to parse start time from state: %w", err)
	}
	desiredTotalRestartCount, err := strconv.Atoi(rawState[stateDesiredTotalRestartCountFieldName])
	if err != nil {
		return WorkloadData{}, fmt.Errorf("failed to parse desired total restart count from state: %w", err)
	}
	state := StateData{
		Name:                     rawState[stateNameFieldName],
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
		State:                       state,
		TotalRestartCountByWorkerId: totalRestartCountByWorkerId,
	}
	return workloadData, nil
}

func (o *Orchestrator) buildValkeyStateKeyName(jobSetUid string) string {
	return fmt.Sprintf("%s:%s:%s", jobSetUidKeyPreffix, jobSetUid, stateKeySuffix)
}

func (o *Orchestrator) buildValkeyRestartCountByWorkerIdKeyName(jobSetUid string) string {
	return fmt.Sprintf("%s:%s:%s", jobSetUidKeyPreffix, jobSetUid, restartCountByWorkerIdKeySuffix)
}

func (o *Orchestrator) buildValkeyTotalRestartCountByWorkerIdKeyName(jobSetUid string) string {
	return fmt.Sprintf("%s:%s:%s", jobSetUidKeyPreffix, jobSetUid, totalRestartCountByWorkerIdKeySuffix)
}

// TODO: Handle request for full recreation
func (o *Orchestrator) handlePollCases(ctx context.Context, jobSetUid string, jobSet jobset.JobSet, workloadData WorkloadData) error {
	klog.Infof("Handling poll cases. jobSetUid=%s", jobSetUid)
	if workloadData.State.Name == stateNameCreating {
		return o.handleWorkloadCreatingCase(ctx, jobSetUid, jobSet, workloadData)
	}
	if workloadData.State.Name == stateNameRunning {
		return o.handleWorkloadRunningCase(ctx, jobSetUid, workloadData)
	}
	if workloadData.State.Name == stateNameRestarting {
		return o.handleWorkloadRestartingCase(ctx, jobSetUid, jobSet, workloadData)
	}
	if workloadData.State.Name == stateNameRecreating {
		return o.handleWorkloadRecreatingCase(ctx, jobSetUid, jobSet, workloadData)
	}
	return fmt.Errorf("unknown workload state: %s", workloadData.State.Name)
}

func (o *Orchestrator) handleWorkloadCreatingCase(ctx context.Context, jobSetUid string, jobSet jobset.JobSet, workloadData WorkloadData) error {
	klog.Infof("Handle workload creating case. jobSetUid=%s", jobSetUid)
	matchingCount := 0
	for workerId, totalRestartCount := range workloadData.TotalRestartCountByWorkerId {
		if totalRestartCount == 0 {
			matchingCount += 1
			continue
		}
		if totalRestartCount < 0 {
			klog.Infof("Detected request for full recreation. Total restart count %d for worker %s. Starting full recreation", totalRestartCount, workerId)
		} else {
			klog.Errorf("Invalid total restart count %d for worker %s. Starting full recreation", totalRestartCount, workerId)
		}
		return o.startFullRecreation(ctx, jobSetUid)
	}
	workerCount := o.countWorkers(jobSet)
	if matchingCount == workerCount {
		return o.startFullRunning(ctx, jobSetUid, workloadData.State.DesiredTotalRestartCount)
	}
	return nil
}

func (o *Orchestrator) countWorkers(jobSet jobset.JobSet) int {
	workerCount := 0
	for _, rjob := range jobSet.Spec.ReplicatedJobs {
		workerCount += int(rjob.Replicas) * int(*rjob.Template.Spec.Parallelism)
	}
	return workerCount
}

func (o *Orchestrator) handleWorkloadRunningCase(ctx context.Context, jobSetUid string, workloadData WorkloadData) error {
	klog.Infof("Handle workload running case. jobSetUid=%s", jobSetUid)
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
			klog.Infof("Detected request for full recreation. Total restart count %d for worker %s. Starting full recreation", totalRestartCount, workerId)
		} else {
			klog.Errorf("Invalid total restart count %d for worker %s. Starting full recreation", totalRestartCount, workerId)
		}
		return o.startFullRecreation(ctx, jobSetUid)
	}
	if requiresRestart {
		return o.startFullRestart(ctx, jobSetUid, workloadData.State.DesiredTotalRestartCount+1)
	}
	return nil
}

func (o *Orchestrator) handleWorkloadRestartingCase(ctx context.Context, jobSetUid string, jobSet jobset.JobSet, workloadData WorkloadData) error {
	klog.Infof("Handle workload restarting case. jobSetUid=%s", jobSetUid)
	now, err := o.getCurrentTimeFromValkey(ctx)
	if err != nil {
		return err
	}
	timeoutDuration, err := o.getTimeoutDuration(jobSet)
	if err != nil {
		return err
	}
	if now.After(workloadData.State.StartTime.Add(timeoutDuration)) {
		klog.Error("Full restart timed out. Starting full recreation")
		return o.startFullRecreation(ctx, jobSetUid)
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
			klog.Infof("Detected request for full recreation. Total restart count %d for worker %s. Starting full recreation", totalRestartCount, workerId)
		} else {
			klog.Errorf("Invalid total restart count %d for worker %s. Starting full recreation", totalRestartCount, workerId)
		}
		return o.startFullRecreation(ctx, jobSetUid)
	}
	workerCount := o.countWorkers(jobSet)
	if matchingCount == workerCount {
		return o.startFullRunning(ctx, jobSetUid, workloadData.State.DesiredTotalRestartCount)
	}
	return nil
}

func (o *Orchestrator) getTimeoutDuration(jobSet jobset.JobSet) (time.Duration, error) {
	rawTimeout, ok := jobSet.Annotations["jobset.sigs.k8s.io/in-place-pod-restart-timeout"]
	if !ok {
		return time.Duration(0), fmt.Errorf("in-place-pod-restart-timeout annotation not found")
	}
	timeout, err := time.ParseDuration(rawTimeout)
	if err != nil {
		return time.Duration(0), fmt.Errorf("failed to parse in-place-pod-restart-timeout annotation: %w", err)
	}
	return timeout, nil
}

func (o *Orchestrator) handleWorkloadRecreatingCase(ctx context.Context, jobSetUid string, jobSet jobset.JobSet, workloadData WorkloadData) error {
	klog.Infof("Handle workload recreating case. jobSetUid=%s", jobSetUid)
	matchingCount := 0
	for workerId, totalRestartCount := range workloadData.TotalRestartCountByWorkerId {
		if totalRestartCount == 0 {
			matchingCount += 1
			continue
		}
		if totalRestartCount < 0 {
			klog.Infof("Detected request for full recreation. Total restart count %d for worker %s. Starting full recreation", totalRestartCount, workerId)
		} else {
			klog.Errorf("Invalid total restart count %d for worker %s. Starting full recreation", totalRestartCount, workerId)
		}
		return o.startFullRecreation(ctx, jobSetUid)
	}
	workerCount := o.countWorkers(jobSet)
	if matchingCount == workerCount {
		return o.startFullRunning(ctx, jobSetUid, workloadData.State.DesiredTotalRestartCount)
	}
	return nil
}

func (o *Orchestrator) startFullRunning(ctx context.Context, jobSetUid string, newDesiredTotalRestartCount int) error {
	klog.Infof("Start full running. jobSetUid=%s", jobSetUid)
	now, err := o.getCurrentTimeFromValkey(ctx)
	if err != nil {
		return err
	}
	err = o.valkeyClient.Do(ctx, o.valkeyClient.B().Hset().Key(o.buildValkeyStateKeyName(jobSetUid)).FieldValue().
		FieldValue(stateNameFieldName, stateNameRunning).
		FieldValue(stateStartTimeFieldName, now.Format(time.RFC3339)).
		FieldValue(stateDesiredTotalRestartCountFieldName, strconv.Itoa(newDesiredTotalRestartCount)).
		Build()).Error()
	if err != nil {
		return fmt.Errorf("failed to update workload state to Valkey: %w", err)
	}
	return nil
}

func (o *Orchestrator) startFullRestart(ctx context.Context, jobSetUid string, newDesiredTotalRestartCount int) error {
	klog.Infof("Start full restart. jobSetUid=%s", jobSetUid)
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
		o.buildValkeyStateKeyName(jobSetUid),
	}
	args := []string{
		stateNameFieldName,
		stateNameRestarting,
		stateStartTimeFieldName,
		now.Format(time.RFC3339),
		stateDesiredTotalRestartCountFieldName,
		strconv.Itoa(newDesiredTotalRestartCount),
		o.valkeyBroadcastChannelName,
		string(jsonData),
	}
	err = script.Exec(ctx, o.valkeyClient, keys, args).Error()
	if err != nil {
		return fmt.Errorf("failed to publish restart signal and update workload state to Valkey: %w", err)
	}
	return nil
}

func (o *Orchestrator) startFullRecreation(ctx context.Context, jobSetUid string) error {
	klog.Infof("Start full recreation. jobSetUid=%s", jobSetUid)
	err := o.deleteChildJobs(ctx, jobSetUid)
	if err != nil {
		return fmt.Errorf("failed to delete child jobs: %w", err)
	}
	err = o.setStateToRecreatingAndRemoveWorkerDataFromValkey(ctx, jobSetUid)
	if err != nil {
		return fmt.Errorf("failed to update workload state and remove worker data from Valkey: %w", err)
	}
	return nil
}

func (o *Orchestrator) deleteChildJobs(ctx context.Context, jobSetUid string) error {
	klog.Infof("Deleting child jobs. jobSetUid=%s", jobSetUid)
	childJobs, err := o.listChildJobs(ctx, jobSetUid)
	if err != nil {
		return fmt.Errorf("failed to list child jobs: %w", err)
	}
	foregroundPolicy := metav1.DeletePropagationForeground
	for _, childJob := range childJobs.Items {
		err := o.kubeClient.BatchV1().Jobs(childJob.Namespace).Delete(ctx, childJob.Name, metav1.DeleteOptions{PropagationPolicy: &foregroundPolicy})
		if err != nil {
			klog.Errorf("Failed to delete child job %q: %v", childJob.Name, err)
		}
	}
	return nil
}

func (o *Orchestrator) listChildJobs(ctx context.Context, jobSetUid string) (*batchv1.JobList, error) {
	labelSelector := fmt.Sprintf("jobset.sigs.k8s.io/jobset-uid=%s", jobSetUid)
	childJobs, err := o.kubeClient.BatchV1().Jobs("").List(ctx, metav1.ListOptions{LabelSelector: labelSelector})
	if err != nil {
		return &batchv1.JobList{}, fmt.Errorf("failed to list jobs: %w", err)
	}
	return childJobs, nil
}

func (o *Orchestrator) setStateToRecreatingAndRemoveWorkerDataFromValkey(ctx context.Context, jobSetUid string) error {
	klog.Infof("Setting state to recreating and removing worker data for JobSet UID %s", jobSetUid)
	now, err := o.getCurrentTimeFromValkey(ctx)
	if err != nil {
		return err
	}
	script := valkey.NewLuaScript(`
	redis.call('HSET', KEYS[1], ARGV[1], ARGV[2], ARGV[3], ARGV[4], ARGV[5], ARGV[6])
	redis.call('DEL', KEYS[2])
	redis.call('DEL', KEYS[3])
	return redis.status_reply("OK")
	`)
	keys := []string{
		o.buildValkeyStateKeyName(jobSetUid),
		o.buildValkeyRestartCountByWorkerIdKeyName(jobSetUid),
		o.buildValkeyTotalRestartCountByWorkerIdKeyName(jobSetUid),
	}
	args := []string{
		stateNameFieldName,
		stateNameRecreating,
		stateStartTimeFieldName,
		now.Format(time.RFC3339),
		stateDesiredTotalRestartCountFieldName,
		"0",
	}
	err = script.Exec(ctx, o.valkeyClient, keys, args).Error()
	if err != nil {
		return fmt.Errorf("failed to update workload state and remove worker data from Valkey: %w", err)
	}
	return nil
}
