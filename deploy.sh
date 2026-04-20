#!/bin/bash
set -euo pipefail

########################################
#  CONFIG
########################################

# Load .env if it exists
if [[ -f .env ]]; then
    set -o allexport
    source .env
    set +o allexport
fi

if [[ -z "${AMI_CONNECT__AWS_PROFILE:-}" ]]; then
    echo "ERROR: AMI_CONNECT__AWS_PROFILE is not set. Add it to your .env file."
    exit 1
fi

# Use the AWS profile as the environment name for deployment purposes
ENVIRONMENT=$AMI_CONNECT__AWS_PROFILE

# Pass in value "restart" to do a full restart of Airflow services, whick kills running DAGs.
FULL_RESTART_ARG="${1:-false}"
if [[ $FULL_RESTART_ARG == "restart" ]]; then
    FULL_RESTART="true"
else
    FULL_RESTART="false"
fi

AMI_CONNECT_REPO="currentsoftwareapp/current-software-ami-connect"
# If you include a private neptune adapter in your deploy,
# set the AMI_CONNECT_NEPTUNE_REPO_URL environment variable before running this script
# Defaults to empty string if not set.
AMI_CONNECT_NEPTUNE_REPO_URL="${AMI_CONNECT_NEPTUNE_REPO_URL:-}"

TERRAFORM_OUTPUT_FILE="./amideploy/configuration/$ENVIRONMENT-output.json"

REMOTE_USER="ec2-user"
REMOTE_DIR="/home/ec2-user/build"
SSH_KEY="./amideploy/configuration/$ENVIRONMENT-airflow-key.pem"

# Read Terraform outputs
AIRFLOW_HOST=$(jq -r '.airflow_server_ip.value' $TERRAFORM_OUTPUT_FILE)
DB_HOST=$(jq -r '.airflow_db_host.value' $TERRAFORM_OUTPUT_FILE)
DB_PASSWORD=$(jq -r '.airflow_db_password.value' $TERRAFORM_OUTPUT_FILE)
AIRFLOW_SITE_URL=$(jq -r '.airflow_site_url.value' $TERRAFORM_OUTPUT_FILE)
UTILITY_BILLING_CONNECTION_URL=$(jq -r '.utility_billing_connection_url.value' $TERRAFORM_OUTPUT_FILE)

AIRFLOW_DB_CONN="postgresql+psycopg2://airflow_user:$DB_PASSWORD@$DB_HOST/airflow_db"

########################################
#  UTILITY FUNCTIONS
########################################

log() {
    echo "[$(date '+%Y-%m-%d %H:%M:%S')] $*"
}

run_ssh() {
    ssh -i "$SSH_KEY" -o StrictHostKeyChecking=no \
        "$REMOTE_USER@$AIRFLOW_HOST" "$1"
}

copy_tree() {
    rsync -avz -e "ssh -i $SSH_KEY -o StrictHostKeyChecking=no" \
        "$1/" "$REMOTE_USER@$AIRFLOW_HOST:$2/"
}

########################################
#  DEPLOYMENT STEPS
########################################

log "===== AMI Connect Airflow Deploy ====="
log "Environment: $ENVIRONMENT"
log "Server: $REMOTE_USER@$AIRFLOW_HOST"
log "Remote directory: $REMOTE_DIR"

log "Ensuring remote directory exists..."
run_ssh "mkdir -p $REMOTE_DIR"

log "Syncing deployment files..."
copy_tree "./amideploy/deploy" "$REMOTE_DIR"

log "Running remote deployment script with FULL_RESTART=$FULL_RESTART..."
run_ssh "cd $REMOTE_DIR && \
    AMI_CONNECT__AIRFLOW_METASTORE_CONN='$AIRFLOW_DB_CONN' \
    AMI_CONNECT__AIRFLOW_SITE_URL='$AIRFLOW_SITE_URL' \
    AMI_CONNECT__UTILITY_BILLING_CONNECTION_URL='$UTILITY_BILLING_CONNECTION_URL' \
    FULL_RESTART='$FULL_RESTART' \
    AMI_CONNECT_REPO='$AMI_CONNECT_REPO' \
    AMI_CONNECT_NEPTUNE_REPO_URL='$AMI_CONNECT_NEPTUNE_REPO_URL' \
    bash remote-deploy.sh"

log "===== Deployment complete ====="
