# Normal Variables
variable "project_id" {
  description = "The Google Cloud project identifier"
  type = string
}

variable "credentials_file" {
  description = "The path to the Google Cloud service account credentials"
  type = string
}

variable "region" {
  description = "The Google Cloud region where the resources will be deployed"
  type = string
}

variable "zone" {
  description = "The Google Cloud zone where the resources will be deployed"
  type = string
}

variable "data_location" {
  description = "The location for storing data, including Google Cloud Storage buckets and Cloud SQL backups"
  type = string
}

variable "database_tier" {
  description = "The machine tier to use for the Academic Observatory Cloud SQL database"
  type = string
}

variable "backup_start_time" {
  description = "The time for Cloud SQL database backups to start in HH:MM format"
  type = string
}

variable "airflow_main_machine_type" {
  description = "The machine type for the Airflow Coordinator virtual machine"
  type = string
}

variable "airflow_worker_machine_type" {
  description = "The machine type for the Airflow Worker virtual machine(s)"
  type = string
}

# Airflow secret variables
variable "fernet_key" {
  description = "The fernet key which is used to encrypt the secrets in the airflow database"
  type = string
}

variable "postgres_password" {
  description = "The password for the airflow postgres database user"
  type = string
}

variable "airflow_ui_user_password" {
  description = "The password for the Apache Airflow UI's airflow user"
  type = string
}

variable "redis_password" {
  description = "The password for redis, which is used by Celery to send messages, e.g. task messages"
  type = string
}

# Airflow connection variables
variable "mag_releases_table_connection" {
  description = "The mag_releases_table connection"
  type = string
}

variable "mag_snapshots_container_connection" {
  description = "The mag_snapshots_container connection"
  type = string
}

variable "crossref_connection" {
  description = "The crossref connection"
  type = string
}
