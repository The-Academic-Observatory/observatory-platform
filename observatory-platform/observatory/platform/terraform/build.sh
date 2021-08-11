#!/usr/bin/env bash

# Load environmental variables with package versions
source ../../.../.env

# Documentation recommends to sleep for 30 seconds first:
sleep 30

# Install Docker
sudo apt-get update
sudo apt-get -y install apt-transport-https ca-certificates curl gnupg-agent software-properties-common
sudo apt-get -y install docker.io
sudo service docker restart

# Make the airflow user and add it to the docker group
sudo useradd --home-dir /home/airflow --shell /bin/bash --create-home airflow
sudo usermod -aG docker airflow
sudo newgrp docker

# Install Docker Compose
url="https://github.com/docker/compose/releases/download/${docker_compose_version}/docker-compose-${os}-${arch}"
sudo rm -f /usr/local/bin/docker-compose
sudo curl -L $url -o /usr/local/bin/docker-compose
sudo chmod +x /usr/local/bin/docker-compose

# Install berglas
url="https://storage.googleapis.com/berglas/${berglas_version}/${os}_${berglas_arch}/berglas"
sudo rm -f /usr/local/bin/berglas
sudo curl -L $url -o /usr/local/bin/berglas
sudo chmod +x /usr/local/bin/berglas

# Install Google Compute Monitoring agent
url="https://dl.google.com/cloudagents/add-monitoring-agent-repo.sh"
curl -sSO $url
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
sudo chown -R airflow:airflow /opt/

# Set working directory and environment variables for building docker containers
cd /opt/observatory/build/docker
export HOST_USER_ID=$(id -u airflow)
export HOST_GROUP_ID=$(id -g airflow)
export HOST_REDIS_PORT=6379
export HOST_FLOWER_UI_PORT=5555
export HOST_AIRFLOW_UI_PORT=8080
export HOST_ELASTIC_PORT=9200
export HOST_KIBANA_PORT=5601

# Pull and build Docker containers
PRESERVE_ENV="HOST_USER_ID,HOST_GROUP_ID,HOST_REDIS_PORT,HOST_FLOWER_UI_PORT,HOST_AIRFLOW_UI_PORT,HOST_ELASTIC_PORT,HOST_KIBANA_PORT"
sudo -u airflow --preserve-env=${PRESERVE_ENV} bash -c "docker-compose -f docker-compose.observatory.yml pull"
sudo -u airflow --preserve-env=${PRESERVE_ENV} bash -c "docker-compose -f docker-compose.observatory.yml build"