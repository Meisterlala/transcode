#!/usr/bin/env pwsh
param (
  [string]$RepoName = "registry.meisterlala.dev/transcoder"
)

$Platforms = @(
  "linux/amd64",
  "linux/arm64"
) -join ","  # Docker expects a comma-separated string

Write-Output "Building Docker image: ${RepoName}:latest for: $Platforms"

# Build and push multi-arch image
docker buildx build `
  --platform $Platforms `
  -t "${RepoName}:latest" `
  --push `
  .

Write-Output "Successfully pushed ${RepoName}:latest"
