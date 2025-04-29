package orchestrator

import (
	"context"
	"fmt"
	"time"

	"github.com/valkey-io/valkey-go"
	"k8s.io/client-go/kubernetes"
	"k8s.io/klog/v2"
	jobsetclientset "sigs.k8s.io/jobset/client-go/clientset/versioned"
)

const (
	maxConnectionRetries = 5
	retryDelay           = 5 * time.Second
)

type Orchestrator struct {
	kubeClient      *kubernetes.Clientset
	jobsetClient    *jobsetclientset.Clientset
	valkeyClient    valkey.Client
	valkeyAddress   string
	pollingInterval time.Duration
	podUid          string
}

func NewOrchestrator(valkeyAddress string, pollingInterval time.Duration, podUid string) (*Orchestrator, error) {
	klog.Info("Creating orchestrator")
	var kubeClient *kubernetes.Clientset
	var jobsetClient *jobsetclientset.Clientset
	var valkeyClient valkey.Client
	var err error
	for i := range maxConnectionRetries {
		kubeClient, jobsetClient, err = connectToKubernetes()
		if err == nil {
			break
		}
		klog.Warningf("Failed to connect to Kubernetes (attempt %d/%d). Retrying in %v: %v", i+1, maxConnectionRetries, retryDelay, err)
		time.Sleep(retryDelay)
	}
	if err != nil {
		return nil, fmt.Errorf("failed to connect to Kubernetes after %d attempts: %w", maxConnectionRetries, err)
	}
	for i := range maxConnectionRetries {
		valkeyClient, err = connectToValkey(valkeyAddress)
		if err == nil {
			break
		}
		klog.Warningf("Failed to connect to Valkey (attempt %d/%d). Retrying in %v: %v", i+1, maxConnectionRetries, retryDelay, err)
		time.Sleep(retryDelay)
	}
	if err != nil {
		return nil, fmt.Errorf("failed to connect to Valkey after %d attempts: %w", maxConnectionRetries, err)
	}
	return &Orchestrator{
		kubeClient:      kubeClient,
		jobsetClient:    jobsetClient,
		valkeyClient:    valkeyClient,
		valkeyAddress:   valkeyAddress,
		pollingInterval: pollingInterval,
		podUid:          podUid,
	}, nil
}

func (o *Orchestrator) Close() {
	klog.Info("Closing orchestrator")
	o.releaseLock(context.Background(), o.podUid)
	o.valkeyClient.Close()
}
