#!/usr/bin/env bash

# Documentation recommends to sleep for 30 seconds first:
sleep 30

# Install Docker
sudo apt-get update
sudo apt-get -y install apt-transport-https ca-certificates curl gnupg-agent software-properties-common
curl -fsSL https://download.docker.com/linux/ubuntu/gpg | sudo apt-key add -
sudo add-apt-repository "deb [arch=amd64] https://download.docker.com/linux/ubuntu bionic stable"
sudo apt-get update
sudo apt-get -y install docker-ce docker-ce-cli containerd.io
sudo service docker restart

# Make the airflow user and add it to the docker group
sudo useradd --home-dir /home/airflow --shell /bin/bash --create-home airflow
sudo usermod -aG docker airflow
sudo newgrp docker

# Install Docker Compose
sudo curl -L "https://github.com/docker/compose/releases/download/1.26.0/docker-compose-Linux-x86_64" -o /usr/local/bin/docker-compose
sudo chmod +x /usr/local/bin/docker-compose

# Install berglas v0.5.3
sudo curl -L "https://storage.googleapis.com/berglas/0.5.0/linux_amd64/berglas" -o /usr/local/bin/berglas
sudo chmod +x /usr/local/bin/berglas

# Move Observatory Platform onto a permanent path
sudo mkdir -p /opt/airflow/logs
sudo mkdir /opt/airflow/dags
sudo mkdir -p /opt/observatory/data
sudo mv /tmp/observatory-platform /opt/observatory/observatory-platform

# Own observatory and airflow directories
sudo chown -R airflow:airflow /opt/observatory/
sudo chown -R airflow:airflow /opt/airflow/

# Build the Docker containers
cd /opt/observatory/observatory-platform
export HOST_USER_ID=$(id -u airflow)
export HOST_GROUP_ID=$(id -g airflow)
sudo -u airflow bash -c "cat docker-compose.cloud.yml docker-compose.observatory.yml > docker-compose.observatory-cloud.yml"
sudo -u airflow bash -c "docker-compose -f docker-compose.observatory-cloud.yml pull"
sudo -u airflow --preserve-env=HOST_USER_ID,HOST_GROUP_ID bash -c "docker-compose -f docker-compose.observatory-cloud.yml build"