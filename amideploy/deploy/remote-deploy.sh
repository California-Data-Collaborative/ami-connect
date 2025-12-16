#!/usr/bin/env bash
set -e

REPO=$AMI_CONNECT_REPO
BRANCH="main"
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
    git reset --hard origin/$BRANCH
    cd $BUILD_DIR
fi

# Pull in the Neptune adapter code if a private neptune repo URL is provided
NEPTUNE_REPO_DIR="$BUILD_DIR/neptune"
if [[ -n "${AMI_CONNECT_NEPTUNE_REPO_URL:-}" ]]; then
    echo "🔧 Pulling latest neptune code from GitHub"
    if [ ! -d $NEPTUNE_REPO_DIR ]; then
        git clone "$AMI_CONNECT_NEPTUNE_REPO_URL" "$NEPTUNE_REPO_DIR"
    else
        cd $NEPTUNE_REPO_DIR
        git fetch --all
        git reset --hard origin/main
        cd $BUILD_DIR
    fi
else
    echo "🔧 No neptune repo configured, creating empty neptune directory"
    # Docker expects the folder to exist, so make an empty one
    mkdir -p "$NEPTUNE_REPO_DIR"
fi

if [[ "${FULL_RESTART,,}" == "true" ]]; then
    echo "🚚 Setting up .env file"
    cd $BUILD_DIR
    [ -f .env ] && rm .env
    echo "AIRFLOW_IMAGE_TAG=$VERSION" >> .env
    # The AMI_CONNECT__AIRFLOW_METASTORE_CONN and other variables are passed from the deploy script on your laptop
    echo "AIRFLOW__CORE__SQL_ALCHEMY_CONN=$AMI_CONNECT__AIRFLOW_METASTORE_CONN" >> .env
    echo "AMI_CONNECT__AIRFLOW_SITE_URL=$AMI_CONNECT__AIRFLOW_SITE_URL" >> .env

    echo "📦 Building Docker image"
    cd "$BUILD_DIR"
    sudo docker build -t airflow:$VERSION .

    echo "🔄 Restarting Docker Compose"
    sudo docker compose up -d

    echo "🧹 Cleaning up old Docker images"
    sudo docker image prune -f
else
    echo "🏃‍♀️ FULL_RESTART is not set to true. Skipping Docker image build and restart."
fi

echo "✅ Deployment complete. Running version: $VERSION"
