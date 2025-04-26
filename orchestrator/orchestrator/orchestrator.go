package orchestrator

import (
	"context"
	"fmt"
	"time"

	"github.com/valkey-io/valkey-go"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/rest"
	"k8s.io/klog/v2"
	jobsetclientset "sigs.k8s.io/jobset/client-go/clientset/versioned"
)

const (
	jobSetUidKeyPreffix                    = "jobset-uid"
	stateKeySuffix                         = "state"
	restartCountByWorkerIdKeySuffix        = "restart-count-by-worker-id"
	totalRestartCountByWorkerIdKeySuffix   = "total-restart-count-by-worker-id"
	stateNameFieldName                     = "name"
	stateStartTimeFieldName                = "start-time"
	stateDesiredTotalRestartCountFieldName = "desired-total-restart-count"
	stateNameCreating                      = "creating"
	stateNameRunning                       = "running"
	stateNameRestarting                    = "restarting"
	stateNameRecreating                    = "recreating"
)

type Orchestrator struct {
	kubeClient                 *kubernetes.Clientset
	jobsetClient               *jobsetclientset.Clientset
	valkeyClient               valkey.Client
	valkeyAddress              string
	valkeyBroadcastChannelName string
	valkeyPollingInterval      time.Duration
}

func NewOrchestrator(valkeyAddress string, valkeyBroadcastChannelName string, valkeyPollingInterval time.Duration) (*Orchestrator, error) {
	klog.Info("Creating orchestrator")
	kubeClient, jobsetClient, err := connectToKubernetes()
	if err != nil {
		return nil, fmt.Errorf("failed to connect to Kubernetes: %w", err)
	}
	valkeyClient, err := connectToValkey(valkeyAddress)
	if err != nil {
		return nil, fmt.Errorf("failed to connect to Valkey: %w", err)
	}
	return &Orchestrator{
		kubeClient:                 kubeClient,
		jobsetClient:               jobsetClient,
		valkeyClient:               valkeyClient,
		valkeyAddress:              valkeyAddress,
		valkeyBroadcastChannelName: valkeyBroadcastChannelName,
		valkeyPollingInterval:      valkeyPollingInterval,
	}, nil
}

func connectToKubernetes() (*kubernetes.Clientset, *jobsetclientset.Clientset, error) {
	klog.Info("Connecting to Kubernetes")
	config, err := rest.InClusterConfig()
	if err != nil {
		return nil, nil, fmt.Errorf("failed to get in-cluster config: %w", err)
	}
	kubeClient, err := kubernetes.NewForConfig(config)
	if err != nil {
		return nil, nil, fmt.Errorf("failed to create Kubernetes client: %w", err)
	}
	jobsetClient, err := jobsetclientset.NewForConfig(config)
	if err != nil {
		return nil, nil, fmt.Errorf("failed to create JobSet client: %w", err)
	}
	return kubeClient, jobsetClient, nil
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

func (o *Orchestrator) Close() {
	klog.Info("Closing orchestrator")
	o.valkeyClient.Close()
}
