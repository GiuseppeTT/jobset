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
	podNamespaceKey           = "POD_NAMESPACE"
	podNameKey                = "POD_NAME"
	podUidKey                 = "POD_UID"
	criSocketPath             = "unix:///run/containerd/containerd.sock"
	criPodUidKey              = "io.kubernetes.pod.uid"
	criContainerNameKey       = "io.kubernetes.container.name"
	restartWorkerStartedAtKey = "alpha.jobset.sigs.k8s.io/restart-worker-started-at"
	workerContainerName       = "worker"
)

func main() {
	log.Printf("INFO: Starting agent")
	kubernetesClient := getKubernetesClient()
	criClient, criConnection := getCriClient()
	defer criConnection.Close()
	podNamespace, podName, podUid := getEnvironmentVariables()
	watchOwnPod(kubernetesClient, criClient, podNamespace, podName, podUid)
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

func getEnvironmentVariables() (string, string, string) {
	podNamespace := os.Getenv(podNamespaceKey)
	if podNamespace == "" {
		log.Fatalf("ERROR: '%s' environment variable must be set", podNamespaceKey)
	}
	podName := os.Getenv(podNameKey)
	if podName == "" {
		log.Fatalf("ERROR: '%s' environment variable must be set", podNameKey)
	}
	podUid := os.Getenv(podUidKey)
	if podName == "" {
		log.Fatalf("ERROR: '%s' environment variable must be set", podUidKey)
	}
	return podNamespace, podName, podUid
}

func watchOwnPod(kubernetesClient *kubernetes.Clientset, criClient runtimeapi.RuntimeServiceClient, podNamespace string, podName string, podUid string) {
	ownPodEventChannel := getOwnPodEventChannel(kubernetesClient, podNamespace, podName)
	for event := range ownPodEventChannel {
		pod, ok := event.Object.(*v1.Pod)
		if !ok {
			log.Fatalf("ERROR: Unexpected object type: %T", event.Object)
		}
		restartWorkerStartedAt, err := getRestartWorkerStartedAt(pod)
		if err != nil {
			log.Printf("WARN: Missing restart worker started at: %v", err)
			continue
		}
		workerContainer, err := getContainer(criClient, podUid)
		if err != nil {
			log.Printf("WARN: Failed to get worker container '%s': %v", workerContainerName, err)
			continue
		}
		workerStartedAt, err := getContainerStartedAt(criClient, workerContainer)
		if err != nil {
			log.Printf("WARN: Failed to get worker container '%s' start timestamp: %v", workerContainerName, err)
			continue
		}
		log.Printf("DEBUG: workerStartedAt=%s", workerStartedAt)
		log.Printf("DEBUG: restartWorkerStartedAt=%s", restartWorkerStartedAt)
		if workerStartedAt.After(restartWorkerStartedAt) {
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

func getOwnPodEventChannel(kubernetesClient *kubernetes.Clientset, podNamespace string, podName string) <-chan watch.Event {
	watcher, err := kubernetesClient.CoreV1().Pods(podNamespace).Watch(context.TODO(), metav1.ListOptions{
		FieldSelector: fmt.Sprintf("metadata.name=%s", podName),
	})
	if err != nil {
		log.Fatalf("ERROR: Failed to watch own Pod '%s/%s': %v", podNamespace, podName, err)
	}
	return watcher.ResultChan()
}

func getRestartWorkerStartedAt(pod *v1.Pod) (time.Time, error) {
	restartWorkerStartedAtRaw, ok := pod.Annotations[restartWorkerStartedAtKey]
	if !ok {
		return time.Time{}, fmt.Errorf("missing '%s' annotation", restartWorkerStartedAtKey)
	}
	restartWorkerStartedAt, err := time.Parse(time.RFC3339, restartWorkerStartedAtRaw)
	if err != nil {
		return time.Time{}, fmt.Errorf("failed to parse '%s' from annotation '%s'", restartWorkerStartedAtRaw, restartWorkerStartedAtKey)
	}
	return restartWorkerStartedAt, nil
}

func getContainer(criClient runtimeapi.RuntimeServiceClient, podUid string) (*runtimeapi.Container, error) {
	listResponse, err := criClient.ListContainers(context.TODO(), &runtimeapi.ListContainersRequest{
		Filter: &runtimeapi.ContainerFilter{
			State: &runtimeapi.ContainerStateValue{
				State: runtimeapi.ContainerState_CONTAINER_RUNNING,
			},
			LabelSelector: map[string]string{
				criPodUidKey:        podUid,
				criContainerNameKey: workerContainerName,
			},
		},
	})
	if err != nil {
		return nil, fmt.Errorf("failed to list containers: %v", err)
	}
	if len(listResponse.Containers) == 0 {
		return nil, fmt.Errorf("no running container '%s' found", workerContainerName)
	}
	if len(listResponse.Containers) > 1 {
		return nil, fmt.Errorf("more than one running container '%s' found", workerContainerName)
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
