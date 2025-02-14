package main

import (
	"context"
	"log"
	"os"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/local"
	runtimeapi "k8s.io/cri-api/pkg/apis/runtime/v1"
)

const (
	criSocketPath = "unix:///run/containerd/containerd.sock"
	pollPeriod    = 1 * time.Second
)

func main() {
	log.Printf("Start")

	ctx := context.Background()
	criClient, criCon := newCriClient(criSocketPath)
	defer criCon.Close()
	podUid, targetContainerName := getEnvVariables()
	watchContainer(ctx, criClient, podUid, targetContainerName, pollPeriod)

	log.Printf("End")
}

func getEnvVariables() (string, string) {
	podUid, podUidExists := os.LookupEnv("POD_UID")
	targetContainerName, targetContainerNameExists := os.LookupEnv("TARGET_CONTAINER_NAME")
	if !podUidExists || !targetContainerNameExists {
		log.Fatalf("Missing POD_UID or TARGET_CONTAINER_NAME")
	}
	return podUid, targetContainerName
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

func watchContainer(ctx context.Context, criClient runtimeapi.RuntimeServiceClient, podUid string, name string, pollPeriod time.Duration) {
	log.Printf("Watch containers %v", name)
	runningContainerId := ""
	for {
		containerStates := getContainerStates(ctx, criClient, podUid, name)
		if runningContainerId == "" {
			newRunningContainerId := getRunningContainerId(containerStates)
			if newRunningContainerId == "" {
				log.Printf("No running container. States: %v", containerStates)
			} else {
				log.Printf("Detected new running container. States: %v", containerStates)
				runningContainerId = newRunningContainerId
			}
		} else {
			runningContainerState, ok := containerStates[runningContainerId]
			if !ok {
				log.Printf("Restarted !!!. States: %v", containerStates)
				runningContainerId = ""
			} else if runningContainerState == runtimeapi.ContainerState_CONTAINER_EXITED {
				log.Printf("Restarted !!!. States: %v", containerStates)
				runningContainerId = ""
			} else if runningContainerState == runtimeapi.ContainerState_CONTAINER_RUNNING {
				log.Printf("Running. States: %v", containerStates)
			} else if runningContainerState == runtimeapi.ContainerState_CONTAINER_UNKNOWN {
				log.Printf("Ex running container is in unkown state. States: %v", containerStates)
			} else if runningContainerState == runtimeapi.ContainerState_CONTAINER_CREATED {
				log.Fatalf("Ex running container is in created state, which should not be possible. States: %v", containerStates)
			}
		}

		time.Sleep(pollPeriod)
	}
}

func getContainerStates(ctx context.Context, criClient runtimeapi.RuntimeServiceClient, podUid string, name string) map[string]runtimeapi.ContainerState {
	// log.Printf("Get state for containers %v", name)
	response, err := criClient.ListContainers(ctx, &runtimeapi.ListContainersRequest{
		Filter: &runtimeapi.ContainerFilter{
			LabelSelector: map[string]string{
				"io.kubernetes.container.name": name,
				"io.kubernetes.pod.uid":        podUid,
			},
		},
	})
	if err != nil {
		log.Fatalf("Failed to get state for containers %v. Failed to list containers: %v", name, err)
	}
	containerStates := make(map[string]runtimeapi.ContainerState)
	for _, container := range response.Containers {
		containerStates[container.Id] = container.State
	}

	return containerStates
}

func getRunningContainerId(containerStates map[string]runtimeapi.ContainerState) string {
	// log.Printf("Get running container id")
	for id, state := range containerStates {
		if state == runtimeapi.ContainerState_CONTAINER_RUNNING {
			return id
		}
	}

	return ""
}
