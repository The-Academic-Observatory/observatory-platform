#!/usr/bin/env bash

# Documentation recommends to sleep for 30 seconds first:
sleep 30

# Install Docker
sudo apt-get update
sudo apt-get -y install apt-transport-https ca-certificates curl gnupg-agent software-properties-common gosu
curl -fsSL https://download.docker.com/linux/ubuntu/gpg | sudo apt-key add -
sudo add-apt-repository "deb [arch=amd64] https://download.docker.com/linux/ubuntu bionic stable"
sudo apt-get update
sudo apt-get -y install docker-ce docker-ce-cli containerd.io
sudo service docker restart

# Make the airflow user and add it to the docker group
sudo useradd --home-dir /home/airflow --shell /bin/bash --create-home airflow
sudo usermod -aG docker airflow
sudo newgrp docker
sudo -u airflow docker run hello-world

# Install Docker Compose
sudo curl -L "https://github.com/docker/compose/releases/download/1.26.0/docker-compose-Linux-x86_64" -o /usr/local/bin/docker-compose
sudo chmod +x /usr/local/bin/docker-compose

# Install berglas v0.5.3
sudo curl -L "https://storage.googleapis.com/berglas/0.5.0/linux_amd64/berglas" -o /usr/local/bin/berglas
sudo chmod +x /usr/local/bin/berglas

# Move Academic Observatory onto a permanent path
sudo mkdir -p /opt/airflow/logs
sudo mkdir /opt/airflow/dags
sudo mkdir -p /opt/observatory/data
sudo mv /tmp/academic-observatory /opt/observatory/academic-observatory

# Own observatory and airflow directories
sudo chown -R airflow:airflow /opt/observatory/
sudo chown -R airflow:airflow /opt/airflow/

# Build the Docker containers
cd /opt/observatory/academic-observatory
export HOST_USER_ID=$(id -u airflow)
sudo -u airflow -E -H bash -c "docker-compose -f docker-compose.observatory.yml pull"
sudo -u airflow -E -H bash -c "docker-compose -f docker-compose.observatory.yml build"