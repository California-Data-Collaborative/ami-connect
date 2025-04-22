#!/bin/bash

# Set these variables before running the script
HOSTNAME=$AMI_CONNECT__AIRFLOW_SERVER_HOSTNAME
PEM_PATH=./amideploy/configuration/airflow-key.pem
PROD_CONFIG=./config.prod.yaml
PROD_SECRETS=./secrets.prod.yaml

# Check if required environment variables are set
if [ -z "$HOSTNAME" ]; then
    echo "Error: Airflow server hostname is not set"
    exit 1
fi

if [ -z "$PEM_PATH" ]; then
    echo "Error: Airflow server pem path is not set"
    exit 1
fi

# Check if config and secrets files exists
if [ ! -f $PROD_CONFIG ]; then
    echo "Error: production config file not found at $PROD_CONFIG"
    exit 1
fi
if [ ! -f $PROD_SECRETS ]; then
    echo "Error: production secrets file not found at $PROD_SECRETS"
    exit 1
fi


echo "Copying files to $HOSTNAME..."

# Perform the SCP operations
scp -i "$PEM_PATH" $PROD_CONFIG "ec2-user@$HOSTNAME:/home/ec2-user/config.yaml"
scp -i "$PEM_PATH" $PROD_SECRETS "ec2-user@$HOSTNAME:/home/ec2-user/secrets.yaml"
scp -i "$PEM_PATH" requirements.txt "ec2-user@$HOSTNAME:/home/ec2-user/"
scp -i "$PEM_PATH" -r amiadapters "ec2-user@$HOSTNAME:/home/ec2-user/"
scp -i "$PEM_PATH" -r amicontrol/dags "ec2-user@$HOSTNAME:/home/ec2-user/"

# Check if the SCP command was successful
echo
if [ $? -eq 0 ]; then
    echo "File copy successful!"
    echo
else
    echo "File copy failed!"
    exit 1
fi

echo "Activating virtual environment and installing requirements..."

# SSH to the server and run the commands
ssh -i "$PEM_PATH" "ec2-user@$HOSTNAME" "source venv/bin/activate && \
    pip install -r requirements.txt"

# Check if the SSH command was successful
if [ $? -eq 0 ]; then
    echo "Successful deploy! Virtual environment activated and requirements installed."
else
    echo "Error: Failed to activate virtual environment or install requirements."
    exit 1
fi