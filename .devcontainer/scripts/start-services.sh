#!/bin/bash
set -e

echo "=== Starting Docker Compose services ==="

cd "$(dirname "$0")/../../"

# Start services
docker-compose up -d

echo "Waiting for services to be healthy..."
sleep 10

echo "=== Services started ==="
