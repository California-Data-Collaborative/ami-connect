#!/bin/bash
# ENV=$1
# TODO make env configurable
AMI_CONNECT__AIRFLOW_SERVER_HOSTNAME=$(jq -r '.airflow_server_ip.value' amideploy/configuration/cadc-output.json)
AMI_CONNECT__AIRFLOW_SERVER_USERNAME=ec2-user
AMI_CONNECT__AIRFLOW_SERVER_PEM=amideploy/configuration/cadc-airflow-key.pem

REMOTE_DIR="./build"

echo "Deploying to Airflow server at $AMI_CONNECT__AIRFLOW_SERVER_USERNAME@$AMI_CONNECT__AIRFLOW_SERVER_HOSTNAME:$REMOTE_DIR using PEM $AMI_CONNECT__AIRFLOW_SERVER_PEM"

# Copy necessary files to the Airflow server
echo "Copying deployment files to Airflow server..."
ssh -i $AMI_CONNECT__AIRFLOW_SERVER_PEM $AMI_CONNECT__AIRFLOW_SERVER_USERNAME@$AMI_CONNECT__AIRFLOW_SERVER_HOSTNAME "mkdir -p ${REMOTE_DIR}"
scp -i $AMI_CONNECT__AIRFLOW_SERVER_PEM ./amideploy/deploy/Dockerfile "$AMI_CONNECT__AIRFLOW_SERVER_USERNAME@$AMI_CONNECT__AIRFLOW_SERVER_HOSTNAME:$REMOTE_DIR/"
scp -i $AMI_CONNECT__AIRFLOW_SERVER_PEM ./amideploy/deploy/docker-compose.yml "$AMI_CONNECT__AIRFLOW_SERVER_USERNAME@$AMI_CONNECT__AIRFLOW_SERVER_HOSTNAME:$REMOTE_DIR/"
scp -i $AMI_CONNECT__AIRFLOW_SERVER_PEM ./amideploy/deploy/deploy.sh "$AMI_CONNECT__AIRFLOW_SERVER_USERNAME@$AMI_CONNECT__AIRFLOW_SERVER_HOSTNAME:$REMOTE_DIR/"

# # Execute the deployment script on the Airflow server
echo "Executing deployment script on Airflow server..."
ssh -i $AMI_CONNECT__AIRFLOW_SERVER_PEM $AMI_CONNECT__AIRFLOW_SERVER_USERNAME@$AMI_CONNECT__AIRFLOW_SERVER_HOSTNAME "bash $REMOTE_DIR/deploy.sh"
