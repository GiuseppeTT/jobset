package agent

import (
	"context"
	"fmt"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/local"
	runtimeapi "k8s.io/cri-api/pkg/apis/runtime/v1"
	"k8s.io/klog/v2"
)

const (
	criPodUidLabelKey            = "io.kubernetes.pod.uid"
	criContainerNameLabelKey     = "io.kubernetes.container.name"
	criRestartCountAnnotationKey = "io.kubernetes.container.restartCount"
)

func connectToCri(socketPath string) (*grpc.ClientConn, runtimeapi.RuntimeServiceClient, error) {
	klog.Info("Connecting to CRI")
	criCon, err := grpc.NewClient(socketPath, grpc.WithTransportCredentials(local.NewCredentials()))
	if err != nil {
		return nil, nil, fmt.Errorf("failed to create gRPC client: %w", err)
	}
	criClient := runtimeapi.NewRuntimeServiceClient(criCon)
	return criCon, criClient, nil
}

func (a *Agent) getContainer(ctx context.Context, podUid string, containerName string) (*runtimeapi.Container, error) {
	klog.V(2).Infof("Getting container with pod UID '%s' and container name '%s'", podUid, containerName)
	listResponse, err := a.criClient.ListContainers(ctx, &runtimeapi.ListContainersRequest{
		Filter: &runtimeapi.ContainerFilter{
			State: &runtimeapi.ContainerStateValue{
				State: runtimeapi.ContainerState_CONTAINER_RUNNING,
			},
			LabelSelector: map[string]string{
				criPodUidLabelKey:        podUid,
				criContainerNameLabelKey: containerName,
			},
		},
	})
	if err != nil {
		return nil, fmt.Errorf("failed to list containers: %w", err)
	}
	if len(listResponse.Containers) == 0 {
		return nil, fmt.Errorf("no running containers found")
	}
	if len(listResponse.Containers) > 1 {
		return nil, fmt.Errorf("more than one running container found")
	}
	return listResponse.Containers[0], nil
}

func (a *Agent) killContainer(ctx context.Context, id string) error {
	klog.Infof("Killing container with ID '%s'", id)
	_, err := a.criClient.StopContainer(ctx, &runtimeapi.StopContainerRequest{
		ContainerId: id,
		Timeout:     int64(a.workerTerminationGracePeriod.Seconds()),
	})
	if err != nil {
		return fmt.Errorf("failed to stop container with ID '%s': %w", id, err)
	}
	return nil
}
