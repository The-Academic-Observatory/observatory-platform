#!/usr/bin/env bash

# Set environment variables
export POSTGRES_HOSTNAME="${postgres_hostname}"
export POSTGRES_PASSWORD="sm://${project_id}/postgres_password"
export REDIS_HOSTNAME="${redis_hostname}"
export FERNET_KEY="sm://${project_id}/fernet_key"

cd /academic-observatory
sudo -E bash -c "berglas exec -- docker-compose -f docker-compose.airflow-worker.yml up -d"
