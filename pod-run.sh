#!/bin/bash

IMAGE="falcondb:latest"
POD="falcondb"
LOGS="$(pwd)/logs"

mkdir -p "$LOGS"

echo "Building image..."
podman build -t $IMAGE .

echo "Creating pod..."
podman pod create --name $POD -p 8000:8000

echo "Starting RP..."
podman run -d --pod $POD --name rp \
  -v "$LOGS":/app/logs \
  $IMAGE node RP/server.js

echo "Starting dn0s1..."
podman run -d --pod $POD --name dn0s1 \
  -v falcondb-dn0s1:/app/DBdata \
  -v "$LOGS":/app/logs \
  $IMAGE node DN/dn0s1/server.js

echo "Starting dn0s2..."
podman run -d --pod $POD --name dn0s2 \
  -v falcondb-dn0s2:/app/DBdata \
  -v "$LOGS":/app/logs \
  $IMAGE node DN/dn0s2/server.js

echo "Starting dn0s3..."
podman run -d --pod $POD --name dn0s3 \
  -v falcondb-dn0s3:/app/DBdata \
  -v "$LOGS":/app/logs \
  $IMAGE node DN/dn0s3/server.js

echo ""
echo "falconDB running -> http://localhost:8000"
echo "Logs -> $LOGS"
