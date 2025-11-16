#!/bin/bash

REPO_NAME=${1:-"registry.meisterlala.dev/transcode"}

echo "Building Docker image: $REPO_NAME:latest"
docker build -t "$REPO_NAME:latest" .

echo "Pushing Docker image: $REPO_NAME:latest"
docker push "$REPO_NAME:latest"

echo "Successfully pushed $REPO_NAME:latest"