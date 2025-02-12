package main

import (
	"context"
	"encoding/json"
	"log"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/local"
	runtimeapi "k8s.io/cri-api/pkg/apis/runtime/v1"
)

const (
	criSocketPath = "unix:///run/containerd/containerd.sock"
	containerName = "worker"
	sleepDuration = 10 * time.Second
)

func main() {
	log.Printf("Start")

	ctx := context.Background()

	criClient, criCon := newCriClient(criSocketPath)
	defer criCon.Close()
	time.Sleep(sleepDuration)
	listAllContainers(ctx, criClient)
	time.Sleep(sleepDuration)
	stopContainer(ctx, criClient, containerName)
	time.Sleep(sleepDuration)
	listAllContainers(ctx, criClient)

	sleepInfinity()

	log.Printf("End")
}

func newCriClient(criSocketPath string) (runtimeapi.RuntimeServiceClient, *grpc.ClientConn) {
	log.Printf("Create client")
	criCon, err := grpc.Dial(criSocketPath, grpc.WithTransportCredentials(local.NewCredentials()))
	if err != nil {
		log.Fatalf("Failed to connect to CRI socket: %v", err)
	}
	criClient := runtimeapi.NewRuntimeServiceClient(criCon)

	return criClient, criCon
}

func listAllContainers(ctx context.Context, criClient runtimeapi.RuntimeServiceClient) {
	log.Printf("List all containers")
	response, err := criClient.ListContainers(ctx, &runtimeapi.ListContainersRequest{})
	if err != nil {
		log.Fatalf("Failed to list containers: %v", err)
	}
	for _, container := range response.Containers {
		prettyContainer, err := json.MarshalIndent(container, "", "  ")
		if err != nil {
			log.Fatalf("Failed to marshal container: %v", err)
		}
		log.Printf("Container: %v", string(prettyContainer))
	}
}

func stopContainer(ctx context.Context, criClient runtimeapi.RuntimeServiceClient, name string) {
	log.Printf("Stop container %v", name)
	id := getContainerId(ctx, criClient, name)
	_, err := criClient.StopContainer(ctx, &runtimeapi.StopContainerRequest{
		ContainerId: id,
		Timeout:     0,
	})
	if err != nil {
		log.Fatalf("Failed to stop container %v: %v", name, err)
	}
}

func getContainerId(ctx context.Context, criClient runtimeapi.RuntimeServiceClient, name string) string {
	log.Printf("Get container %v", name)
	response, err := criClient.ListContainers(ctx, &runtimeapi.ListContainersRequest{
		Filter: &runtimeapi.ContainerFilter{
			State: &runtimeapi.ContainerStateValue{
				State: runtimeapi.ContainerState_CONTAINER_RUNNING,
			},
			LabelSelector: map[string]string{
				"io.kubernetes.container.name": name,
			},
		},
	})
	if err != nil {
		log.Fatalf("Failed to get container %v: %v", name, err)
	}
	id := response.Containers[0].Id

	return id
}

func sleepInfinity() {
	log.Printf("Sleep infinity")
	for {
		log.Printf("I'm alive")
		time.Sleep(60 * time.Second)
	}
}
