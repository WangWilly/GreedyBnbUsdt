#/bin/bash

IMAGE="greedybnbusdt/backend"
COMMIT_HASH=$(git rev-parse --short HEAD)

docker build \
-t ${IMAGE}:$COMMIT_HASH \
-f ./Dockerfile \
.

docker tag ${IMAGE}:$COMMIT_HASH ${IMAGE}:latest
