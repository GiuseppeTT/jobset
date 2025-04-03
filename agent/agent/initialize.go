package agent

import (
	"context"
	"fmt"
	"net/http"

	"k8s.io/klog/v2"

	"github.com/redis/go-redis/v9"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/local"
	runtimeapi "k8s.io/cri-api/pkg/apis/runtime/v1"
)

func (a *Agent) Initialize(ctx context.Context) error {
	klog.Infof("Initializing agent")
	err := a.connectToCri()
	if err != nil {
		return fmt.Errorf("failed to connect to CRI: %w", err)
	}
	err = a.connectToRedis(ctx)
	if err != nil {
		return fmt.Errorf("failed to connect Redis: %w", err)
	}
	err = a.subscribeToBroadcastChannel(ctx)
	if err != nil {
		return fmt.Errorf("failed to subscribe to broadcast channel: %w", err)
	}
	err = a.handleInitializationCases(ctx)
	if err != nil {
		return fmt.Errorf("failed to handle initialization cases: %w", err)
	}
	go a.serveReadyProbe()
	return nil
}

func (a *Agent) connectToCri() error {
	klog.Infof("Connecting to CRI")
	criCon, err := grpc.NewClient(a.criSocketPath, grpc.WithTransportCredentials(local.NewCredentials()))
	if err != nil {
		return fmt.Errorf("failed to create gRCP client: %w", err)
	}
	criClient := runtimeapi.NewRuntimeServiceClient(criCon)
	a.criCon = criCon
	a.criClient = criClient
	return nil
}

func (a *Agent) connectToRedis(ctx context.Context) error {
	klog.Infof("Connecting to Redis")
	redisClient := redis.NewClient(&redis.Options{
		Addr: a.redisAddress,
		DB:   0,
	})
	_, err := redisClient.Ping(ctx).Result()
	if err != nil {
		return fmt.Errorf("failed to ping Redis: %w", err)
	}
	a.redisClient = redisClient
	return nil
}

func (a *Agent) subscribeToBroadcastChannel(ctx context.Context) error {
	klog.Infof("Subscribing to broadcast channel")
	redisSubscription := a.redisClient.Subscribe(ctx, a.redisBroadcastChannelName)
	_, err := redisSubscription.Receive(ctx)
	if err != nil {
		return fmt.Errorf("failed to subscribe to broadcast channel: %w", err)
	}
	a.redisSubscription = redisSubscription
	return nil
}

func (a *Agent) handleInitializationCases(ctx context.Context) error {
	klog.Infof("Handling initialization cases")
	restartCount, totalRestartCount, err := a.getCountVariablesFromRedis(ctx)
	if err != nil {
		return err
	}
	if totalRestartCount == nil {
		return a.handlePodCreatedCase(ctx)
	}
	if *totalRestartCount < 0 {
		return fmt.Errorf("total restart count cannot be negative")
	}
	startedFileExists, err := a.startedFileExists()
	if err != nil {
		return err
	}
	if !startedFileExists {
		return a.handlePodCreatedIndepententlyCase(ctx, totalRestartCount)
	}
	return a.handleAgentRestartedCase(restartCount, totalRestartCount)
}

func (a *Agent) handlePodCreatedCase(ctx context.Context) error {
	klog.Infof("Handling pod created case")
	newRestartCount := intPtr(0)
	newTotalRestartCount := intPtr(0)
	err := a.createStartedFile()
	if err != nil {
		return err
	}
	err = a.initializeCountVariablesToRedis(ctx, newRestartCount, newTotalRestartCount)
	if err != nil {
		return err
	}
	a.restartCount = newRestartCount
	a.totalRestartCount = newTotalRestartCount
	return nil
}

func (a *Agent) handlePodCreatedIndepententlyCase(ctx context.Context, redisTotalRestartCount *int) error {
	klog.Infof("Handling pod created independently case")
	newRestartCount := intPtr(0)
	newTotalRestartCount := intPtr(*redisTotalRestartCount + 1)
	err := a.updateCountVariablesToRedis(ctx, newRestartCount, newTotalRestartCount)
	if err != nil {
		return err
	}
	err = a.createStartedFile()
	if err != nil {
		return err
	}
	a.restartCount = newRestartCount
	a.totalRestartCount = newTotalRestartCount
	return nil
}

func (a *Agent) handleAgentRestartedCase(redisRestartCount *int, redisTotalRestartCount *int) error {
	klog.Infof("Handling agent restarted case")
	a.restartCount = redisRestartCount
	a.totalRestartCount = redisTotalRestartCount
	return nil
}

func (a *Agent) serveReadyProbe() {
	klog.Infof("Serving ready probe")
	http.HandleFunc(readyProbeHttpPath, func(w http.ResponseWriter, r *http.Request) {})
	address := fmt.Sprintf(":%d", readyProbePort)
	err := http.ListenAndServe(address, nil)
	if err != nil {
		klog.Errorf("Failed to serve ready probe: %v", err)
	}
}

func (a *Agent) Close() {
	klog.Infof("Closing agent")
	a.criCon.Close()
	a.redisSubscription.Close()
	a.redisClient.Close()
}
