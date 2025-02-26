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
# Remember to change the agent image in the manifest
# Remember to link kubectl to your k8s cluster
kubectl apply -f fail-fail-success-jobset.yaml
```

# Check output

```bash
kubectl get deployments
kubectl get jobset
kubectl get jobs
kubectl get pods

kubectl logs pods/<pod name> -c agent
```