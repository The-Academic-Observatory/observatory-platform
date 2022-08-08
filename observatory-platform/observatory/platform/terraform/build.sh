#!/usr/bin/env bash

# Documentation recommends to sleep for 30 seconds first:
sleep 30

# Install Docker
sudo apt-get update
sudo apt-get -y install ca-certificates curl gnupg lsb-release
sudo mkdir -p /etc/apt/keyrings
curl -fsSL https://download.docker.com/linux/ubuntu/gpg | sudo gpg --dearmor -o /etc/apt/keyrings/docker.gpg
echo "deb [arch=$(dpkg --print-architecture) signed-by=/etc/apt/keyrings/docker.gpg] https://download.docker.com/linux/ubuntu $(lsb_release -cs) stable" | sudo tee /etc/apt/sources.list.d/docker.list > /dev/null
sudo apt-get update
sudo apt-get -y install docker-ce docker-ce-cli containerd.io docker-compose-plugin
sudo service docker restart

# Make the airflow user and add it to the docker group
sudo useradd --home-dir /home/airflow --shell /bin/bash --create-home airflow
sudo usermod -aG docker airflow
sudo newgrp docker

# Install berglas v0.6.2
sudo curl -L "https://storage.googleapis.com/berglas/0.6.2/linux_amd64/berglas" -o /usr/local/bin/berglas
sudo chmod +x /usr/local/bin/berglas

# Install Google Compute Monitoring agent
curl -sSO https://dl.google.com/cloudagents/add-monitoring-agent-repo.sh
sudo bash add-monitoring-agent-repo.sh
sudo apt-get update
sudo apt-get install -y 'stackdriver-agent=6.*'
sudo service stackdriver-agent start

# Make directories
sudo mkdir -p /opt/airflow/logs
sudo mkdir /opt/airflow/dags
sudo mkdir -p /opt/observatory/data
sudo mkdir -p /opt/observatory/build/docker

# Move all packages into /opt directory
sudo cp -r /tmp/opt/packages/* /opt

# Move Docker files into /opt/observatory/build/docker directory
sudo cp -r /tmp/opt/observatory/build/docker/* /opt/observatory/build/docker

# Remove tmp
sudo rm -r /tmp

# Own all /opt directories
sudo chown -R airflow /opt/

# Set working directory and environment variables for building docker containers
cd /opt/observatory/build/docker
export HOST_USER_ID=$(id -u airflow)
export HOST_REDIS_PORT=6379
export HOST_FLOWER_UI_PORT=5555
export HOST_AIRFLOW_UI_PORT=8080
export HOST_ELASTIC_PORT=9200
export HOST_KIBANA_PORT=5601

# Pull and build Docker containers
PRESERVE_ENV="HOST_USER_ID,HOST_REDIS_PORT,HOST_FLOWER_UI_PORT,HOST_AIRFLOW_UI_PORT,HOST_ELASTIC_PORT,HOST_KIBANA_PORT"
sudo -u airflow --preserve-env=${PRESERVE_ENV} bash -c "docker compose -f docker-compose.observatory.yml pull"
sudo -u airflow --preserve-env=${PRESERVE_ENV} bash -c "docker compose -f docker-compose.observatory.yml build"