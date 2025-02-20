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
	podUid, targetContainerName := getEnvVariables()
	criClient, criCon := newCriClient(criSocketPath)
	defer criCon.Close()
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
	log.Printf("Watch containers with name '%v'", name)
	trackedContainerId := ""
	for {
		containerSummaries := getContainerSummaries(ctx, criClient, podUid, name)
		log.Printf("Container summaries: %v", containerSummaries)

		if trackedContainerId == "" {
			runningContainerId := getRunningContainerId(containerSummaries)
			if runningContainerId == "" {
				log.Printf("No running container")
				time.Sleep(pollPeriod)
				continue
			} else {
				log.Printf("Detected new running container")
				trackedContainerId = runningContainerId
			}
		}

		trackedContainerSummary, ok := containerSummaries[trackedContainerId]
		if !ok {
			log.Fatalf("Lost tracked container")
		} else if trackedContainerSummary.state == runtimeapi.ContainerState_CONTAINER_RUNNING {
			log.Printf("Running")
		} else if trackedContainerSummary.state == runtimeapi.ContainerState_CONTAINER_UNKNOWN {
			log.Printf("Unkown state")
		} else if trackedContainerSummary.state == runtimeapi.ContainerState_CONTAINER_CREATED {
			log.Fatalf("Error: Container went from running to created state")
		} else if trackedContainerSummary.state == runtimeapi.ContainerState_CONTAINER_EXITED && trackedContainerSummary.exitCode == 0 {
			log.Printf("Finished succesfully !!!")
			log.Printf("Finishing the job")
			break // This make the program finish, which makes the Pod complete, which makes the Job complete
		} else if trackedContainerSummary.state == runtimeapi.ContainerState_CONTAINER_EXITED && trackedContainerSummary.exitCode == 1 {
			log.Printf("Failed !!!")
			log.Printf("Broadcasting restart signal")
			trackedContainerId = ""
		} else {
			log.Fatalf("Unexpected case")
		}

		time.Sleep(pollPeriod)
	}
}

type containerSummary struct {
	id       string
	state    runtimeapi.ContainerState
	exitCode int32
}

func getContainerSummaries(ctx context.Context, criClient runtimeapi.RuntimeServiceClient, podUid string, name string) map[string]containerSummary {
	// log.Printf("Get summary of containers with name '%v'", name)
	listResponse, err := criClient.ListContainers(ctx, &runtimeapi.ListContainersRequest{
		Filter: &runtimeapi.ContainerFilter{
			LabelSelector: map[string]string{
				"io.kubernetes.container.name": name,
				"io.kubernetes.pod.uid":        podUid,
			},
		},
	})
	if err != nil {
		log.Fatalf("Failed to get summary of containers with name '%v'. Failed to list containers: %v", name, err)
	}
	containerSummaries := make(map[string]containerSummary)
	for _, container := range listResponse.Containers {
		statusResponse, err := criClient.ContainerStatus(ctx, &runtimeapi.ContainerStatusRequest{
			ContainerId: container.Id,
		})
		if err != nil {
			log.Fatalf("Failed to get summary of containers with name '%v'. Failed to get status: %v", name, err)
		}
		containerSummaries[container.Id] = containerSummary{
			id:       container.Id,
			state:    container.State,
			exitCode: statusResponse.Status.ExitCode,
		}
	}

	return containerSummaries
}

func getRunningContainerId(containerSummaries map[string]containerSummary) string {
	// log.Printf("Get running container id")
	for id, summary := range containerSummaries {
		if summary.state == runtimeapi.ContainerState_CONTAINER_RUNNING {
			return id
		}
	}

	return ""
}
