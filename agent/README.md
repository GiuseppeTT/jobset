# How to build

```bash
REGISTRY=""
IMAGE=""
TAG=""
docker build -t ${REGISTRY}/${IMAGE}:${TAG} .
docker push ${REGISTRY}/${IMAGE}:${TAG}
```