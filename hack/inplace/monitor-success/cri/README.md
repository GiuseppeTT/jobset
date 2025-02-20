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
# Remember to change the agent image in monitor.yaml
# Remember to link kubectl to your k8s cluster
kubectl apply -f monitor.yaml
```

# Check output

```bash
kubectl get jobs
kubectl get pods

kubectl logs jobs/fail-fail-success-stop -c worker
kubectl logs jobs/fail-fail-success-stop -c agent
```

# Debug

```bash
kubectl exec -it fail-fail-success-stop -c agent -- /bin/sh
```