# Build docker image

```bash
REGISTRY="<CHANGE ME>"
IMAGE="<CHANGE ME>"
TAG="<CHANGE ME>"
docker build --pull --no-cache -t ${REGISTRY}/${IMAGE}:${TAG} .
docker push ${REGISTRY}/${IMAGE}:${TAG}
```

# Apply manifest

```bash
# Remember to change the agent image in broadcast.yaml
# Remember to link kubectl to your k8s cluster
kubectl apply -f broadcast.yaml
```

# Check output

```bash
kubectl get deployments
kubectl get pods

kubectl logs pods/redis-<COMPLETE>
kubectl logs pods/agent-<COMPLETE 1>
kubectl logs pods/agent-<COMPLETE 2>
```

# Debug

```bash
kubectl exec -it <pod-name> -- /bin/sh
```