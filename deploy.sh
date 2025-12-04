#!/bin/bash
set -euo pipefail

########################################
#  CONFIG
########################################

if [[ $# -lt 1 ]]; then
    echo "ERROR: Missing required argument: ENVIRONMENT"
    echo "Usage: $0 <environment> [restart] where <environment> matches the name of your terraform environment so the script can pull details from your terraform output files."
    echo "Example: $0 cadc"
    echo "Or for full restart: $0 cadc restart"
    exit 1
fi

ENVIRONMENT="$1"

# Pass in value "restart" to do a full restart of Airflow services, whick kills running DAGs.
FULL_RESTART_ARG="${2:-false}"
if [[ $FULL_RESTART_ARG == "restart" ]]; then
    FULL_RESTART="true"
else
    FULL_RESTART="false"
fi

TERRAFORM_OUTPUT_FILE="./amideploy/configuration/$ENVIRONMENT-output.json"

REMOTE_USER="ec2-user"
REMOTE_DIR="/home/ec2-user/build"
SSH_KEY="./amideploy/configuration/$ENVIRONMENT-airflow-key.pem"

# Read Terraform outputs
AIRFLOW_HOST=$(jq -r '.airflow_server_ip.value' $TERRAFORM_OUTPUT_FILE)
DB_HOST=$(jq -r '.airflow_db_host.value' $TERRAFORM_OUTPUT_FILE)
DB_PASSWORD=$(jq -r '.airflow_db_password.value' $TERRAFORM_OUTPUT_FILE)

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

# Optional: if you need the neptune directory
if [ -d "/home/ec2-user/neptune" ]; then
    log "Copying local neptune folder..."
    # copy_tree "/home/ec2-user/neptune" "$REMOTE_DIR/neptune"
fi

log "Running remote deployment script with FULL_RESTART=$FULL_RESTART..."
run_ssh "cd $REMOTE_DIR && \
    AMI_CONNECT__AIRFLOW_METASTORE_CONN='$AIRFLOW_DB_CONN' \
    FULL_RESTART='$FULL_RESTART' \
    bash remote-deploy.sh"

log "===== Deployment complete ====="
