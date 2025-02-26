package agent

import (
	"context"
	"encoding/json"
	"time"

	"k8s.io/klog/v2"

	"github.com/google/uuid"
	"github.com/redis/go-redis/v9"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/local"
	runtimeapi "k8s.io/cri-api/pkg/apis/runtime/v1"
)

const (
	criContainerNameLabelKey    = "io.kubernetes.container.name"
	criPodUidLabelKey           = "io.kubernetes.pod.uid"
	redisBroadcastChannelName   = "broadcast-channel"
	redisLockKey                = "lock-key"
	ExitCodeSuccess             = 0
	ExitCodeGeneralError        = 1
	ExitCodeTerminatedBySigkill = 137
	ExitCodeTerminatedBySigterm = 143
)

type Agent struct {
	// Target container
	targetPodUid        string
	targetContainerName string
	targetContainerId   string
	// CRI
	criSocketPath      string
	criPollingInterval time.Duration
	criCon             *grpc.ClientConn
	criClient          runtimeapi.RuntimeServiceClient
	// Redis
	redisAddress      string
	redisLockTtl      time.Duration
	redisLockInterval time.Duration
	redisClient       *redis.Client
	redisSubscription *redis.PubSub
}

func NewAgent(
	targetPodUid string,
	targetContainerName string,
	criSocketPath string,
	criPollingInterval time.Duration,
	redisAddress string,
	redisLockTtl time.Duration,
	redisLockInterval time.Duration,
) *Agent {
	return &Agent{
		// Target container
		targetPodUid:        targetPodUid,
		targetContainerName: targetContainerName,
		targetContainerId:   "",
		// CRI
		criSocketPath:      criSocketPath,
		criPollingInterval: criPollingInterval,
		criCon:             nil,
		criClient:          nil,
		// Redis
		redisAddress:      redisAddress,
		redisLockTtl:      redisLockTtl,
		redisLockInterval: redisLockInterval,
		redisClient:       nil,
		redisSubscription: nil,
	}
}

func (a *Agent) Initialize(ctx context.Context) {
	klog.Infof("Initializing agent")
	a.initializeCriClient()
	a.initializeRedisClient()
	a.subscribeToBroadcastChannel(ctx)
}

func (a *Agent) initializeCriClient() {
	criCon, err := grpc.NewClient(a.criSocketPath, grpc.WithTransportCredentials(local.NewCredentials()))
	if err != nil {
		klog.Fatalf("Failed to connect to CRI socket: %v", err)
	}
	criClient := runtimeapi.NewRuntimeServiceClient(criCon)
	a.criCon = criCon
	a.criClient = criClient
}

func (a *Agent) initializeRedisClient() {
	redisClient := redis.NewClient(&redis.Options{
		Addr: a.redisAddress,
		DB:   0,
	})
	_, err := redisClient.Ping(context.Background()).Result()
	if err != nil {
		klog.Fatalf("Failed to connect to Redis: %v", err)
	}
	a.redisClient = redisClient
}

func (a *Agent) subscribeToBroadcastChannel(ctx context.Context) {
	redisSubscription := a.redisClient.Subscribe(ctx, redisBroadcastChannelName)
	_, err := redisSubscription.Receive(ctx)
	if err != nil {
		klog.Fatalf("Failed to subscribe to broadcast channel: %v", err)
	}
	a.redisSubscription = redisSubscription
}

func (a *Agent) Close() {
	klog.Infof("Closing agent")
	a.criCon.Close()
	a.redisSubscription.Close()
	a.redisClient.Close()
}

type messageData struct {
	Id     uuid.UUID `json:"id"`
	Time   time.Time `json:"time"`
	PodUid string    `json:"podUid"`
}

func (a *Agent) Run(ctx context.Context) {
	klog.Infof("Running agent")
	ticker := time.NewTicker(a.criPollingInterval)
	defer ticker.Stop()
	broadcastChannel := a.redisSubscription.Channel()

	for {
		select {
		case <-ticker.C:
			shouldStop := a.checkTargetContainer(ctx)
			if shouldStop {
				return
			}
		case message := <-broadcastChannel:
			a.handleMessage(ctx, message)
		case <-ctx.Done():
			return
		}
	}
}

func (a *Agent) checkTargetContainer(ctx context.Context) bool {
	containerStatuses, err := a.getContainerStatuses(ctx)
	if err != nil {
		klog.Errorf("Failed to get container statuses: %v", err)
		return false
	}
	if a.targetContainerId == "" {
		targetRunningContainerId := a.extractTargetRunningContainerId(containerStatuses)
		if targetRunningContainerId == "" {
			klog.Infof("No target running container")
			return false
		} else {
			klog.Infof("Detected new target running container")
			a.targetContainerId = targetRunningContainerId
		}
	}

	status, ok := containerStatuses[a.targetContainerId]
	if !ok { // This should not happen because a.targetContainerId is set to "" when target container is exited. Maybe a problem in polling speed?
		klog.Errorf("Target container status not found")
		return false
	}
	switch status.State {
	case runtimeapi.ContainerState_CONTAINER_RUNNING:
		klog.Infof("Target container is running")
	case runtimeapi.ContainerState_CONTAINER_UNKNOWN:
		klog.Errorf("Target container is in unkown state")
	case runtimeapi.ContainerState_CONTAINER_CREATED:
		klog.Errorf("Target container changed from running to created state")
	case runtimeapi.ContainerState_CONTAINER_EXITED:
		switch status.ExitCode {
		case ExitCodeSuccess: // Agent should exit 0 to succeed the pod
			klog.Infof("Target container finished succesfully !!!")
			return true
		case ExitCodeGeneralError:
			klog.Infof("Target container failed !!!")
			a.targetContainerId = ""
			a.attemptLockAndBroadcast(ctx)
		case ExitCodeTerminatedBySigkill, ExitCodeTerminatedBySigterm:
			klog.Infof("Target container was killed by agent !!!")
			a.targetContainerId = ""
		default:
			klog.Errorf("Unexpected exit code '%v'. Target container status: %v", status.ExitCode, status)
		}
	default:
		klog.Errorf("Unexpected container state '%v'. Target container status: %v", status.State, status)
	}

	return false
}

func (a *Agent) getContainerStatuses(ctx context.Context) (map[string]*runtimeapi.ContainerStatus, error) {
	listResponse, err := a.criClient.ListContainers(ctx, &runtimeapi.ListContainersRequest{
		Filter: &runtimeapi.ContainerFilter{
			LabelSelector: map[string]string{
				criContainerNameLabelKey: a.targetContainerName,
				criPodUidLabelKey:        a.targetPodUid,
			},
		},
	})
	if err != nil {
		klog.Errorf("Failed to list containers: %v", err)
		return nil, err
	}
	containerStatuses := make(map[string]*runtimeapi.ContainerStatus)
	for _, container := range listResponse.Containers {
		statusResponse, err := a.criClient.ContainerStatus(ctx, &runtimeapi.ContainerStatusRequest{
			ContainerId: container.Id,
		})
		if err != nil { // Possible race condition
			klog.Errorf("Failed to get container status: %v", err)
			return nil, err
		}
		containerStatuses[container.Id] = statusResponse.Status
	}

	return containerStatuses, nil
}

func (a *Agent) extractTargetRunningContainerId(containerStatuses map[string]*runtimeapi.ContainerStatus) string {
	runningContainerId := ""
	for id, status := range containerStatuses {
		if status.State == runtimeapi.ContainerState_CONTAINER_RUNNING {
			if runningContainerId != "" {
				klog.Errorf("Found multiple target running containers")
			}
			runningContainerId = id
		}
	}

	return runningContainerId
}

func (a *Agent) attemptLockAndBroadcast(ctx context.Context) {
	acquired := a.acquireLock(ctx)
	if !acquired {
		return
	}
	a.broadcast(ctx)
	a.releaseLock(ctx)
}

func (a *Agent) acquireLock(ctx context.Context) bool {
	acquired, err := a.redisClient.SetNX(ctx, redisLockKey, a.targetPodUid, a.redisLockTtl).Result()
	if err != nil {
		klog.Errorf("Failed to aquire lock due to error: %v", err)
		return false
	}
	if !acquired {
		klog.Infof("Failed to acquire lock")
		return false
	}
	klog.Infof("Acquired lock")

	return true
}

func (a *Agent) broadcast(ctx context.Context) {
	uuid, err := uuid.NewRandom()
	if err != nil {
		klog.Errorf("Failed to generate message UUID: %v", err)
		return
	}
	data := messageData{
		Id:     uuid,
		Time:   time.Now(),
		PodUid: a.targetPodUid,
	}
	jsonData, err := json.Marshal(data)
	if err != nil {
		klog.Errorf("Failed to marshall message data to JSON: %v", err)
		return
	}
	err = a.redisClient.Publish(ctx, redisBroadcastChannelName, string(jsonData)).Err()
	if err != nil {
		klog.Errorf("Failed to broadcast message: %v", err)
		return
	}
	time.Sleep(a.redisLockInterval) // Give some time for the broadcast before releasing the lock
}

func (a *Agent) releaseLock(ctx context.Context) {
	err := a.redisClient.Del(ctx, redisLockKey).Err()
	if err != nil {
		klog.Errorf("Failed to release lock: %v", err)
		return
	}
	klog.Infof("Released lock")
}

func (a *Agent) handleMessage(ctx context.Context, message *redis.Message) {
	var data messageData
	err := json.Unmarshal([]byte(message.Payload), &data)
	if err != nil {
		klog.Errorf("Failed to unmarshall message data from JSON: %v", err)
		return
	}
	if data.PodUid == a.targetPodUid { // Message was sent by this agent
		return
	}
	if a.targetContainerId == "" { // There is no container to restart
		return
	}
	a.killTargetContainer(ctx)
}

func (a *Agent) killTargetContainer(ctx context.Context) {
	_, err := a.criClient.StopContainer(ctx, &runtimeapi.StopContainerRequest{
		ContainerId: a.targetContainerId,
		Timeout:     0,
	})
	if err != nil {
		klog.Errorf("Failed to kill target container %v", err)
	}
	klog.Infof("Killed target container")
}
