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

# Install Docker Compose
sudo curl -L "https://github.com/docker/compose/releases/download/1.26.0/docker-compose-Linux-x86_64" -o /usr/local/bin/docker-compose
sudo chmod +x /usr/local/bin/docker-compose

# Install berglas v0.5.3
sudo curl -L "https://storage.googleapis.com/berglas/master/linux_amd64/berglas" -o /usr/local/bin/berglas
sudo chmod +x /usr/local/bin/berglas

# Move Academic Observatory onto a permanent path
sudo mv /tmp/academic-observatory /academic-observatory