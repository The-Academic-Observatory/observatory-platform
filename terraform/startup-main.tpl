#!/usr/bin/env bash

# Set environment variables
export POSTGRES_HOSTNAME="${postgres_hostname}"
export POSTGRES_PASSWORD="sm://${project_id}/postgres_password"
export FERNET_KEY="sm://${project_id}/fernet_key"

# Run program
cd /academic-observatory
sudo -E bash -c "berglas exec -- docker-compose -f docker-compose.airflow-main.yml up -d"
