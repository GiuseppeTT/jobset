package main

import (
	"context"
	"fmt"
	"log"
	"os"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	v1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/watch"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/rest"
	runtimeapi "k8s.io/cri-api/pkg/apis/runtime/v1"
)

const (
	criSocketPath            = "unix:///run/containerd/containerd.sock"
	criPodUidLabelKey        = "io.kubernetes.pod.uid"
	criContainerNameLabelKey = "io.kubernetes.container.name"
	jobSetNamespace          = "jobset-system"
	RestartStartedAtDataKey  = "RestartStartedAt"
)

func main() {
	log.Printf("INFO: Starting agent")
	kubernetesClient := getKubernetesClient()
	criClient, criConnection := getCriClient()
	defer criConnection.Close()
	jobSetName, podUid, workerContainerName := getEnvVars()
	watchConfigMap(kubernetesClient, criClient, jobSetName, podUid, workerContainerName)
}

func getKubernetesClient() *kubernetes.Clientset {
	config, err := rest.InClusterConfig()
	if err != nil {
		log.Fatalf("ERROR: Failed to get in-cluster config: %v", err)
	}
	kubernetesClient, err := kubernetes.NewForConfig(config)
	if err != nil {
		log.Fatalf("ERROR: Failed to create kubernetes client: %v", err)
	}
	return kubernetesClient
}

func getCriClient() (runtimeapi.RuntimeServiceClient, *grpc.ClientConn) {
	criConnection, err := grpc.NewClient(criSocketPath, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		log.Fatalf("ERROR: Failed to connect to CRI socket: %v", err)
	}
	criClient := runtimeapi.NewRuntimeServiceClient(criConnection)
	return criClient, criConnection
}

func getEnvVars() (string, string, string) {
	jobSetName := os.Getenv("JOBSET_NAME")
	if jobSetName == "" {
		log.Fatalf("ERROR: 'JOBSET_NAME' env var must be set")
	}
	podUid := os.Getenv("POD_UID")
	if podUid == "" {
		log.Fatalf("ERROR: 'POD_NAME' env var must be set")
	}
	workerContainerName := os.Getenv("WORKER_CONTAINER_NAME")
	if workerContainerName == "" {
		log.Fatalf("ERROR: 'WORKER_CONTAINER_NAME' env var must be set")
	}
	return jobSetName, podUid, workerContainerName
}

func watchConfigMap(kubernetesClient *kubernetes.Clientset, criClient runtimeapi.RuntimeServiceClient, jobSetName string, podUid string, workerContainerName string) {
	configMapEventChannel := getConfigMapEventChannel(kubernetesClient, jobSetName)
	for event := range configMapEventChannel {
		restartStartedAt, err := getRestartStartedAt(event)
		if err != nil {
			log.Printf("ERROR: Failed to get restart start timestamp: %v", err)
			continue
		}
		log.Printf("INFO: restartStartedAt=%s", restartStartedAt)
		workerContainer, err := getContainer(criClient, podUid, workerContainerName)
		if err != nil {
			log.Printf("ERROR: Failed to get worker container: %v", err)
			continue
		}
		containerStartedAt, err := getContainerStartedAt(criClient, workerContainer)
		if err != nil {
			log.Printf("ERROR: Failed to get container start timestamp: %v", err)
			continue
		}
		log.Printf("DEBUG: containerStartedAt=%s", containerStartedAt)
		if containerStartedAt.After(restartStartedAt) {
			log.Printf("INFO: Worker container '%s' started after restart start. No op", workerContainerName)
			continue
		}
		log.Printf("INFO: Killing worker container '%s'", workerContainerName)
		err = killContainer(criClient, workerContainer)
		if err != nil {
			log.Printf("ERROR: Failed to kill worker container '%s': %v", workerContainerName, err)
			continue
		}
	}
}

func getConfigMapEventChannel(kubernetesClient *kubernetes.Clientset, jobSetName string) <-chan watch.Event {
	configMapName := jobSetName + "-rg-broadcast"
	watcher, err := kubernetesClient.CoreV1().ConfigMaps("default").Watch(context.TODO(), metav1.ListOptions{ // TODO: Get namespace from env by Pod namespace
		FieldSelector: fmt.Sprintf("metadata.name=%s", configMapName),
	})
	if err != nil {
		log.Fatalf("ERROR: Failed to watch configMap '%s': %v", configMapName, err)
	}
	return watcher.ResultChan()
}

func getRestartStartedAt(event watch.Event) (time.Time, error) {
	configMap, ok := event.Object.(*v1.ConfigMap)
	if !ok {
		return time.Time{}, fmt.Errorf("unexpected object type: %T", event.Object)
	}
	rawRestartStartedAt, ok := configMap.Data[RestartStartedAtDataKey]
	if !ok {
		return time.Time{}, fmt.Errorf("'%s' not found in configMap", RestartStartedAtDataKey)
	}
	restartStartedAt, err := time.Parse(time.RFC3339, rawRestartStartedAt)
	if err != nil {
		return time.Time{}, fmt.Errorf("failed to parse restart start timestamp: %v", err)
	}
	return restartStartedAt, nil
}

func getContainer(criClient runtimeapi.RuntimeServiceClient, podUid string, containerName string) (*runtimeapi.Container, error) {
	listResponse, err := criClient.ListContainers(context.TODO(), &runtimeapi.ListContainersRequest{
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
		return nil, fmt.Errorf("failed to list containers: %v", err)
	}
	if len(listResponse.Containers) == 0 {
		return nil, fmt.Errorf("no running container '%s' found", containerName)
	}
	if len(listResponse.Containers) > 1 {
		return nil, fmt.Errorf("more than one running container '%s' found", containerName)
	}
	container := listResponse.Containers[0]
	return container, nil
}

func getContainerStartedAt(criClient runtimeapi.RuntimeServiceClient, container *runtimeapi.Container) (time.Time, error) {
	statusResponse, err := criClient.ContainerStatus(context.TODO(), &runtimeapi.ContainerStatusRequest{
		ContainerId: container.Id,
	})
	if err != nil {
		return time.Time{}, fmt.Errorf("failed to get container status: %v", err)
	}
	rawStartTimestamp := statusResponse.Status.StartedAt
	if rawStartTimestamp == 0 {
		return time.Time{}, fmt.Errorf("startedAt field not specified in container status")
	}
	startTimestamp := time.Unix(0, rawStartTimestamp)
	return startTimestamp, nil
}

func killContainer(criClient runtimeapi.RuntimeServiceClient, container *runtimeapi.Container) error {
	_, err := criClient.StopContainer(context.TODO(), &runtimeapi.StopContainerRequest{
		ContainerId: container.Id,
	})
	return err
}
