#!/usr/bin/env bash

echo " ----- Sleeping for 30 seconds as per Packer documentation ----- "
sleep 30

echo " ----- Install Docker and Docker Compose V2 (using apt-get) ----- "
sudo apt-get update
sudo apt-get -y install ca-certificates curl gnupg lsb-release
sudo mkdir -p /etc/apt/keyrings
curl -fsSL https://download.docker.com/linux/ubuntu/gpg | sudo gpg --dearmor -o /etc/apt/keyrings/docker.gpg
echo "deb [arch=$(dpkg --print-architecture) signed-by=/etc/apt/keyrings/docker.gpg] https://download.docker.com/linux/ubuntu $(lsb_release -cs) stable" | sudo tee /etc/apt/sources.list.d/docker.list > /dev/null
sudo apt-get update
sudo apt-get -y install docker-ce docker-ce-cli containerd.io docker-compose-plugin
sudo service docker restart

echo " ----- Make the airflow user and add it to the docker group ----- "
sudo useradd --home-dir /home/airflow --shell /bin/bash --create-home airflow
sudo usermod -aG docker airflow
sudo newgrp docker

echo " ----- Install Berglas v1.0.1 ----- "
# See here for a list of releases: https://github.com/GoogleCloudPlatform/berglas/releases
curl -L "https://github.com/GoogleCloudPlatform/berglas/releases/download/v1.0.1/berglas_1.0.1_linux_amd64.tar.gz" | sudo tar -xz -C /usr/local/bin berglas
sudo chmod +x /usr/local/bin/berglas

echo " ----- Install Google Compute Ops Agent ----- "
curl -sSO https://dl.google.com/cloudagents/add-google-cloud-ops-agent-repo.sh
sudo bash add-google-cloud-ops-agent-repo.sh --also-install --version=2.*.*
sudo systemctl status google-cloud-ops-agent"*"

echo " ----- Make airflow and docker directories, move packages, and clean up files ----- "
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

# Own all /opt directories and packer home folder
sudo chown -R airflow /opt/
sudo chown -R airflow /home/packer/

# Set working directory and environment variables for building docker containers
cd /opt/observatory/build/docker
export HOST_USER_ID=$(id -u airflow)
export HOST_REDIS_PORT=6379
export HOST_FLOWER_UI_PORT=5555
export HOST_AIRFLOW_UI_PORT=8080
export HOST_ELASTIC_PORT=9200
export HOST_KIBANA_PORT=5601
export HOST_DATA_PATH=/opt/observatory/data
export HOST_LOGS_PATH=/opt/airflow/logs
export HOST_GOOGLE_APPLICATION_CREDENTIALS=/opt/observatory/google_application_credentials.json
export HOST_API_SERVER_PORT=5002

echo " ----- Building docker containers with docker-compose, running as airflow user ----- "
PRESERVE_ENV="HOST_USER_ID,HOST_REDIS_PORT,HOST_FLOWER_UI_PORT,HOST_AIRFLOW_UI_PORT,HOST_ELASTIC_PORT,HOST_KIBANA_PORT,HOST_API_SERVER_PORT,HOST_DATA_PATH,HOST_LOGS_PATH,HOST_GOOGLE_APPLICATION_CREDENTIALS"
sudo -u airflow --preserve-env=${PRESERVE_ENV} bash -c "docker compose -f docker-compose.observatory.yml pull"
sudo -u airflow --preserve-env=${PRESERVE_ENV} bash -c "docker compose -f docker-compose.observatory.yml build"