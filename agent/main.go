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
	criSocketPath                     = "unix:///run/containerd/containerd.sock"
	criPodUidKey                      = "io.kubernetes.pod.uid"
	criContainerNameKey               = "io.kubernetes.container.name"
	restartWorkerStartedBeforeOrAtKey = "restartWorkerStartedBeforeOrAt"
)

func main() {
	log.Printf("INFO: Starting agent")
	kubernetesClient := getKubernetesClient()
	criClient, criConnection := getCriClient()
	defer criConnection.Close()
	podNamespace, podUid, restartGroupName, workerContainerName := getEnvVars()
	watchBroadcastConfigMap(kubernetesClient, criClient, podNamespace, podUid, restartGroupName, workerContainerName)
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

func getEnvVars() (string, string, string, string) {
	podNamespace := os.Getenv("POD_NAMESPACE")
	if podNamespace == "" {
		log.Fatalf("ERROR: 'POD_NAMESPACE' env var must be set")
	}
	podUid := os.Getenv("POD_UID")
	if podUid == "" {
		log.Fatalf("ERROR: 'POD_UID' env var must be set")
	}
	restartGroupName := os.Getenv("RESTART_GROUP_NAME")
	if restartGroupName == "" {
		log.Fatalf("ERROR: 'RESTART_GROUP_NAME' env var must be set")
	}
	workerContainerName := os.Getenv("WORKER_CONTAINER_NAME")
	if workerContainerName == "" {
		log.Fatalf("ERROR: 'WORKER_CONTAINER_NAME' env var must be set")
	}
	return podNamespace, podUid, restartGroupName, workerContainerName
}

func watchBroadcastConfigMap(kubernetesClient *kubernetes.Clientset, criClient runtimeapi.RuntimeServiceClient, podNamespace string, podUid string, restartGroupName string, workerContainerName string) {
	broadcastConfigMapEventChannel := getBroadcastConfigMapEventChannel(kubernetesClient, podNamespace, restartGroupName)
	for event := range broadcastConfigMapEventChannel {
		restartWorkerStartedBeforeOrAt, err := getRestartWorkerStartedBeforeOrAt(event)
		if err != nil {
			log.Printf("ERROR: Failed to get group restart start timestamp: %v", err)
			continue
		}
		log.Printf("DEBUG: restartWorkerStartedBeforeOrAt: %s", restartWorkerStartedBeforeOrAt)
		workerContainer, err := getContainer(criClient, podUid, workerContainerName)
		if err != nil {
			log.Printf("ERROR: Failed to get worker container '%s': %v", workerContainerName, err)
			continue
		}
		workerStartedAt, err := getContainerStartedAt(criClient, workerContainer)
		if err != nil {
			log.Printf("ERROR: Failed to get worker container '%s' start timestamp: %v", workerContainerName, err)
			continue
		}
		log.Printf("DEBUG: workerStartedAt: %s", workerStartedAt)
		if !(workerStartedAt.Before(restartWorkerStartedBeforeOrAt) || workerStartedAt.Equal(restartWorkerStartedBeforeOrAt)) {
			log.Printf("INFO: Worker container '%s' started after restart worker started before or at. No op", workerContainerName)
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

func getBroadcastConfigMapEventChannel(kubernetesClient *kubernetes.Clientset, podNamespace string, restartGroupName string) <-chan watch.Event {
	broadcastConfigMapName := restartGroupName + "-broadcast"
	watcher, err := kubernetesClient.CoreV1().ConfigMaps(podNamespace).Watch(context.TODO(), metav1.ListOptions{
		FieldSelector: fmt.Sprintf("metadata.name=%s", broadcastConfigMapName),
	})
	if err != nil {
		log.Fatalf("ERROR: Failed to watch broadcast ConfigMap '%s': %v", broadcastConfigMapName, err)
	}
	return watcher.ResultChan()
}

func getRestartWorkerStartedBeforeOrAt(event watch.Event) (time.Time, error) {
	configMap, ok := event.Object.(*v1.ConfigMap)
	if !ok {
		return time.Time{}, fmt.Errorf("unexpected object type: %T", event.Object)
	}
	rawRestartWorkerStartedBeforeOrAt, ok := configMap.Data[restartWorkerStartedBeforeOrAtKey]
	if !ok {
		return time.Time{}, fmt.Errorf("'%s' not found in ConfigMap data", restartWorkerStartedBeforeOrAtKey)
	}
	restartWorkerStartedBeforeOrAt, err := time.Parse(time.RFC3339, rawRestartWorkerStartedBeforeOrAt)
	if err != nil {
		return time.Time{}, fmt.Errorf("failed to parse restart start timestamp: %v", err)
	}
	return restartWorkerStartedBeforeOrAt, nil
}

func getContainer(criClient runtimeapi.RuntimeServiceClient, podUid string, containerName string) (*runtimeapi.Container, error) {
	listResponse, err := criClient.ListContainers(context.TODO(), &runtimeapi.ListContainersRequest{
		Filter: &runtimeapi.ContainerFilter{
			State: &runtimeapi.ContainerStateValue{
				State: runtimeapi.ContainerState_CONTAINER_RUNNING,
			},
			LabelSelector: map[string]string{
				criPodUidKey:        podUid,
				criContainerNameKey: containerName,
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
	startedAt := startTimestamp.Truncate(time.Second)
	return startedAt, nil
}

func killContainer(criClient runtimeapi.RuntimeServiceClient, container *runtimeapi.Container) error {
	_, err := criClient.StopContainer(context.TODO(), &runtimeapi.StopContainerRequest{
		ContainerId: container.Id,
	})
	return err
}
