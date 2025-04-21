# Common parameters
PREFIX="<CHANGE ME>"
PROJECT="<CHANGE ME>"
REGION="<CHANGE ME>"
ZONE="<CHANGE ME>"

ARTIFACT_REPOSITORY="${PREFIX?}-ar"
REGISTRY="${REGION?}-docker.pkg.dev/${PROJECT?}/${ARTIFACT_REPOSITORY?}"

NETWORK="${PREFIX?}-network"
SUBNET="${PREFIX?}-subnet"
FIREWALL_PREFIX="${PREFIX?}-fw"
ROUTER="${PREFIX?}-router"
NAT="${PREFIX?}-nat"

CLUSTER="${PREFIX?}-cluster"
NODE_POOL_PREFIX="${PREFIX?}-np"

# Registry
gcloud artifacts repositories create \
    "${ARTIFACT_REPOSITORY?}" \
    --project="${PROJECT?}" \
    --location="${REGION?}" \
    --repository-format="docker"

# Network
gcloud compute networks create \
    "${NETWORK?}" \
    --project="${PROJECT?}" \
    --subnet-mode="custom"

# Subnet
gcloud compute networks subnets create \
    "${SUBNET?}" \
    --project="${PROJECT?}" \
    --network="${NETWORK?}" \
    --region="${REGION?}" \
    --enable-private-ip-google-access \
    --range="10.100.0.0/15" \
    --secondary-range=pods-range="10.0.0.0/10" \
    --secondary-range=services-range="10.200.0.0/20"

# Firewall
gcloud compute firewall-rules create \
    "${FIREWALL_PREFIX?}-allow-internal" \
    --project="${PROJECT?}" \
    --network="${NETWORK?}" \
    --allow="tcp,udp,icmp"

# Router
gcloud compute routers create \
    "${ROUTER?}" \
    --project="${PROJECT?}" \
    --network="${NETWORK?}" \
    --region="${REGION?}"

# NAT
gcloud compute routers nats create \
    "${NAT?}" \
    --project="${PROJECT?}" \
    --router="${ROUTER?}" \
    --region="${REGION?}" \
    --auto-allocate-nat-external-ips \
    --nat-all-subnet-ip-ranges

# Cluster
gcloud container clusters create \
    "${CLUSTER?}" \
    --project="${PROJECT?}" \
    --location="${ZONE?}" \
    --network="${NETWORK?}" \
    --subnetwork="${SUBNET?}" \
    --cluster-secondary-range-name="pods-range" \
    --services-secondary-range-name="services-range" \
    --enable-ip-alias \
    --enable-private-nodes \
    --monitoring="NONE" \
    --enable-master-authorized-networks \
    --master-authorized-networks="$(curl ifconfig.me)/32"

# Link kubectl
gcloud container clusters get-credentials \
    "${CLUSTER?}" \
    --project="${PROJECT?}" \
    --location="${ZONE?}"

# Node pool
for i in {0..4}; do
    gcloud container node-pools create \
        "${NODE_POOL_PREFIX?}-${i?}" \
        --project="${PROJECT?}" \
        --location="${ZONE?}" \
        --cluster="${CLUSTER?}" \
        --num-nodes="1000" \
        --machine-type="e2-medium" \
        --disk-size="10" \
        --async
done

# Delete default node pool
# (Optional)
gcloud container node-pools delete \
    "default-pool" \
    --project="${PROJECT?}" \
    --location="${ZONE?}" \
    --cluster="${CLUSTER?}" \
    --async