package orchestrator

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	"k8s.io/klog/v2"
	jobset "sigs.k8s.io/jobset/api/jobset/v1alpha2"
)

const (
	stateNameCreating   = "creating"
	stateNameRunning    = "running"
	stateNameRestarting = "restarting"
	stateNameRecreating = "recreating"
)

func (o *Orchestrator) Run(ctx context.Context) {
	klog.Info("Running orchestrator")
	for {
		acquired, err := o.acquireLock(ctx, o.podUid)
		if err != nil {
			klog.Errorf("Failed to acquire lock: %v", err)
			return
		}
		if acquired {
			break
		}
		klog.V(2).Infof("Failed to acquire lock. Retrying in %v", retryDelay)
		time.Sleep(retryDelay)
	}
	ticker := time.NewTicker(o.pollingInterval)
	defer ticker.Stop()
	for {
		select {
		case <-ticker.C:
			err := o.renewLock(ctx, o.podUid)
			if err != nil {
				klog.Errorf("Failed to renew lock: %v", err)
				return
			}
			o.pollAllJobSets(ctx)
		case <-ctx.Done():
			return
		}
	}
}

func (o *Orchestrator) pollAllJobSets(ctx context.Context) {
	klog.V(2).Info("Polling all JobSets")
	newJobSetUids, currentJobSetUids, oldJobSetUids, jobSetByUid, err := o.listJobSets(ctx)
	if err != nil {
		klog.Errorf("Failed to list JobSets: %v", err)
		return
	}
	// TODO: Consider starting concurrently and waiting for all at the end
	for _, jobSetUid := range newJobSetUids {
		o.addJobSetToValkey(ctx, jobSetUid)
	}
	for _, jobSetUid := range oldJobSetUids {
		o.removeJobSetFromValkey(ctx, jobSetUid)
	}
	for _, jobSetUid := range currentJobSetUids {
		jobSet := jobSetByUid[jobSetUid]
		o.pollJobSet(ctx, jobSetUid, jobSet)
	}
}

func (o *Orchestrator) listJobSets(ctx context.Context) ([]string, []string, []string, map[string]jobset.JobSet, error) {
	klog.V(2).Info("Listing JobSets")
	kubeJobSetUids, jobSetByUid, err := o.listJobSetsFromKube(ctx)
	if err != nil {
		return []string{}, []string{}, []string{}, map[string]jobset.JobSet{}, fmt.Errorf("failed to list JobSets from k8s: %w", err)
	}
	valkeyJobSetUids, err := o.listJobSetsFromValkey(ctx)
	if err != nil {
		return []string{}, []string{}, []string{}, map[string]jobset.JobSet{}, fmt.Errorf("failed to list JobSets from Valkey: %w", err)
	}
	newJobSetUids, currentJobSetUids, oldJobSetUids := partitionSets(kubeJobSetUids, valkeyJobSetUids)
	return newJobSetUids, currentJobSetUids, oldJobSetUids, jobSetByUid, nil
}

func (o *Orchestrator) addJobSetToValkey(ctx context.Context, jobSetUid string) {
	klog.Infof("Adding JobSet to Valkey. JobSet UID=%s", jobSetUid)
	now, err := o.getCurrentTimeFromValkey(ctx)
	if err != nil {
		klog.Errorf("Failed to get current time from Valkey: %v", err)
		return
	}
	// TODO: Maybe use initialize (fail if already exists)
	err = o.updateJobSetStateToValkey(ctx, jobSetUid, stateNameCreating, now, 0)
	if err != nil {
		klog.Errorf("Failed to add JobSet to Valkey: %v", err)
	}
}

func (o *Orchestrator) removeJobSetFromValkey(ctx context.Context, jobSetUid string) {
	klog.Infof("Removing JobSet from Valkey. JobSet UID=%s", jobSetUid)
	o.deleteJobSetStateFromValkey(ctx, jobSetUid)
}

func (o *Orchestrator) pollJobSet(ctx context.Context, jobSetUid string, jobSet jobset.JobSet) {
	klog.V(2).Infof("Polling JobSet. JobSet UID=%s", jobSetUid)
	jobSetData, err := o.getJobSetDataFromValkey(ctx, jobSetUid)
	if err != nil {
		klog.Errorf("Failed to get JobSet data from Valkey: %v", err)
		return
	}
	err = o.handlePollCases(ctx, jobSetUid, jobSet, jobSetData)
	if err != nil {
		klog.Errorf("Failed to handle poll cases: %v", err)
		return
	}
}

func (o *Orchestrator) handlePollCases(ctx context.Context, jobSetUid string, jobSet jobset.JobSet, jobSetData JobSetData) error {
	klog.V(2).Infof("Handling poll cases. jobSet UID=%s", jobSetUid)
	if jobSetData.State.Name == stateNameCreating {
		return o.handleJobSetCreatingCase(ctx, jobSetUid, jobSet, jobSetData)
	}
	if jobSetData.State.Name == stateNameRunning {
		return o.handleJobSetRunningCase(ctx, jobSetUid, jobSetData)
	}
	if jobSetData.State.Name == stateNameRestarting {
		return o.handleJobSetRestartingCase(ctx, jobSetUid, jobSet, jobSetData)
	}
	if jobSetData.State.Name == stateNameRecreating {
		return o.handleJobSetRecreatingCase(ctx, jobSetUid, jobSet, jobSetData)
	}
	return fmt.Errorf("unknown state: %s", jobSetData.State.Name)
}

func (o *Orchestrator) handleJobSetCreatingCase(ctx context.Context, jobSetUid string, jobSet jobset.JobSet, jobSetData JobSetData) error {
	klog.V(2).Infof("Handle JobSet creating case. JobSet UID=%s", jobSetUid)
	matchingCount := 0
	for workerId, totalRestartCount := range jobSetData.TotalRestartCountByWorkerId {
		if totalRestartCount < 0 {
			klog.Infof("Detected request for full recreation. Total restart count '%d' for worker '%s'. Starting full recreation", totalRestartCount, workerId)
			return o.startFullRecreation(ctx, jobSetUid)
		}
		if totalRestartCount == 0 {
			matchingCount += 1
			continue
		}
		klog.Errorf("Detected invalid total restart count '%d' for worker '%s' with desired total restart count '%d'. Starting full recreation", totalRestartCount, workerId, jobSetData.State.DesiredTotalRestartCount)
		return o.startFullRecreation(ctx, jobSetUid)
	}
	workerCount := o.countWorkers(jobSet)
	if matchingCount == workerCount {
		return o.startFullRunning(ctx, jobSetUid, jobSetData.State.DesiredTotalRestartCount)
	}
	return nil
}

func (o *Orchestrator) handleJobSetRunningCase(ctx context.Context, jobSetUid string, jobSetData JobSetData) error {
	klog.V(2).Infof("Handle JobSet running case. JobSet UID=%s", jobSetUid)
	requiresRestart := false
	for workerId, totalRestartCount := range jobSetData.TotalRestartCountByWorkerId {
		if totalRestartCount < 0 {
			klog.Infof("Detected request for full recreation. Total restart count '%d' for worker '%s'. Starting full recreation", totalRestartCount, workerId)
			return o.startFullRecreation(ctx, jobSetUid)
		}
		if totalRestartCount == jobSetData.State.DesiredTotalRestartCount {
			continue
		}
		if totalRestartCount == jobSetData.State.DesiredTotalRestartCount+1 {
			requiresRestart = true
			continue
		}
		klog.Errorf("Detected invalid total restart count '%d' for worker '%s' with desired total restart count '%d'. Starting full recreation", totalRestartCount, workerId, jobSetData.State.DesiredTotalRestartCount)
		return o.startFullRecreation(ctx, jobSetUid)
	}
	if requiresRestart {
		return o.startFullRestart(ctx, jobSetUid, jobSetData.State.DesiredTotalRestartCount+1)
	}
	return nil
}

func (o *Orchestrator) handleJobSetRestartingCase(ctx context.Context, jobSetUid string, jobSet jobset.JobSet, jobSetData JobSetData) error {
	klog.V(2).Infof("Handle JobSet restarting case. JobSet UID=%s", jobSetUid)
	now, err := o.getCurrentTimeFromValkey(ctx)
	if err != nil {
		return fmt.Errorf("failed to get current time from Valkey: %w", err)
	}
	timeoutDuration, err := o.getTimeoutDuration(jobSet)
	if err != nil {
		return fmt.Errorf("failed to get timeout duration from JobSet: %w", err)
	}
	if now.After(jobSetData.State.StartTime.Add(timeoutDuration)) {
		klog.Error("Full restart timed out. Starting full recreation")
		return o.startFullRecreation(ctx, jobSetUid)
	}
	matchingCount := 0
	for workerId, totalRestartCount := range jobSetData.TotalRestartCountByWorkerId {
		if totalRestartCount < 0 {
			klog.Infof("Detected request for full recreation. Total restart count '%d' for worker '%s'. Starting full recreation", totalRestartCount, workerId)
			return o.startFullRecreation(ctx, jobSetUid)
		}
		if totalRestartCount == jobSetData.State.DesiredTotalRestartCount-1 {
			continue
		}
		if totalRestartCount == jobSetData.State.DesiredTotalRestartCount {
			matchingCount += 1
			continue
		}
		klog.Errorf("Detected invalid total restart count '%d' for worker '%s' with desired total restart count '%d'. Starting full recreation", totalRestartCount, workerId, jobSetData.State.DesiredTotalRestartCount)
		return o.startFullRecreation(ctx, jobSetUid)
	}
	workerCount := o.countWorkers(jobSet)
	if matchingCount == workerCount {
		return o.startFullRunning(ctx, jobSetUid, jobSetData.State.DesiredTotalRestartCount)
	}
	return nil
}

func (o *Orchestrator) handleJobSetRecreatingCase(ctx context.Context, jobSetUid string, jobSet jobset.JobSet, jobSetData JobSetData) error {
	klog.V(2).Infof("Handle JobSet recreating case. JobSet UID=%s", jobSetUid)
	matchingCount := 0
	for workerId, totalRestartCount := range jobSetData.TotalRestartCountByWorkerId {
		if totalRestartCount < 0 {
			klog.Infof("Detected request for full recreation. Total restart count '%d' for worker '%s'. Starting full recreation", totalRestartCount, workerId)
			return o.startFullRecreation(ctx, jobSetUid)
		}
		if totalRestartCount == 0 {
			matchingCount += 1
			continue
		}
		klog.Errorf("Detected invalid total restart count '%d' for worker '%s' with desired total restart count '%d'. Starting full recreation", totalRestartCount, workerId, jobSetData.State.DesiredTotalRestartCount)
		return o.startFullRecreation(ctx, jobSetUid)
	}
	workerCount := o.countWorkers(jobSet)
	if matchingCount == workerCount {
		return o.startFullRunning(ctx, jobSetUid, jobSetData.State.DesiredTotalRestartCount)
	}
	return nil
}

func (o *Orchestrator) startFullRunning(ctx context.Context, jobSetUid string, newDesiredTotalRestartCount int) error {
	klog.Infof("Start full running. JobSet UID=%s", jobSetUid)
	now, err := o.getCurrentTimeFromValkey(ctx)
	if err != nil {
		return fmt.Errorf("failed to get current time from Valkey: %w", err)
	}
	err = o.updateJobSetStateToValkey(ctx, jobSetUid, stateNameRunning, now, newDesiredTotalRestartCount)
	if err != nil {
		return fmt.Errorf("failed to update JobSet state to Valkey: %w", err)
	}
	return nil
}

func (o *Orchestrator) startFullRestart(ctx context.Context, jobSetUid string, newDesiredTotalRestartCount int) error {
	klog.Infof("Start full restart. JobSet UID=%s", jobSetUid)
	now, err := o.getCurrentTimeFromValkey(ctx)
	if err != nil {
		return fmt.Errorf("failed to get current time from Valkey: %w", err)
	}
	data := MessageData{DesiredTotalRestartCount: newDesiredTotalRestartCount}
	jsonData, err := json.Marshal(data)
	if err != nil {
		return fmt.Errorf("failed to marshall message data to JSON: %w", err)
	}
	err = o.updateJobSetStateAndPublishMessageToValkey(ctx, jobSetUid, stateNameRestarting, now, newDesiredTotalRestartCount, jsonData)
	if err != nil {
		return fmt.Errorf("failed to update JobSet state and publish message to Valkey: %w", err)
	}
	return nil
}

// TODO: Make full recreations count towards jobset.status.restarts and jobset.spec.failurePolicy.maxRestarts
func (o *Orchestrator) startFullRecreation(ctx context.Context, jobSetUid string) error {
	klog.Infof("Start full recreation. JobSet UID=%s", jobSetUid)
	now, err := o.getCurrentTimeFromValkey(ctx)
	if err != nil {
		return fmt.Errorf("failed to get current time from Valkey: %w", err)
	}
	err = o.deleteChildJobs(ctx, jobSetUid)
	if err != nil {
		return fmt.Errorf("failed to delete child jobs: %w", err)
	}
	err = o.updateJobSetStateAndDeleteCountsToValkey(ctx, jobSetUid, stateNameRecreating, now, 0)
	if err != nil {
		return fmt.Errorf("failed to update JobSet state and delete counts to Valkey: %w", err)
	}
	return nil
}
