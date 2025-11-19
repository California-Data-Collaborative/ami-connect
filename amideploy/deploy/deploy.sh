#!/usr/bin/env bash
set -e

# TODO should be configurable
REPO="California-Data-Collaborative/ami-connect.git"
REPO_URL="https://github.com/$REPO"
BUILD_DIR="/home/ec2-user/build"
REPO_DIR="$BUILD_DIR/repo"
VERSION=$(date +"%Y%m%d-%H%M")

echo "🔧 Pulling latest code from GitHub"
if [ ! -d "$REPO_DIR" ]; then
    git clone "$REPO_URL" "$REPO_DIR"
else
    cd "$REPO_DIR"
    git fetch --all
    git reset --hard origin/main
fi

echo "📦 Building Docker image"
cd "$BUILD_DIR"
sudo docker build -t airflow:$VERSION .

echo "🚚 Updating Airflow image tag"
echo "AIRFLOW_IMAGE_TAG=$VERSION" > .env

echo "🔄 Restarting Docker Compose (no interruption to running tasks)"
sudo docker compose up -d

echo "🧹 Cleaning up old Docker images"
docker image prune -f

echo "✅ Deployment complete. Running version: $VERSION"
