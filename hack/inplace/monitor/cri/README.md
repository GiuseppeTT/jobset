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
# Remember to change the agent image in simple-test.yaml
# Remember to link kubectl to your k8s cluster
kubectl apply -f simple-test.yaml
```

# Check output

```bash
kubectl get jobs
kubectl get pods

kubectl logs jobs/simple-test -c worker
kubectl logs jobs/simple-test -c agent
```

# Debug

```bash
kubectl exec -it simple-test -c agent -- /bin/sh
```