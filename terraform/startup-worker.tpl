#!/usr/bin/env bash

# Save google application credentials to file
sudo -E bash -c "berglas access sm://${project_id}/google_application_credentials | base64 --decode > /academic-observatory/google_application_credentials.json"

# Set environment variables
export POSTGRES_HOSTNAME="${postgres_hostname}"
export POSTGRES_PASSWORD="sm://${project_id}/postgres_password"
export REDIS_HOSTNAME="${redis_hostname}"
export FERNET_KEY="sm://${project_id}/fernet_key"

# Run program
cd /academic-observatory
sudo -E bash -c "berglas exec -- docker-compose -f docker-compose.airflow-secrets.yml -f docker-compose.airflow-worker.yml pull"
sudo -E bash -c "berglas exec -- docker-compose -f docker-compose.airflow-secrets.yml -f docker-compose.airflow-worker.yml up -d"
