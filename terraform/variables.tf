# Normal Variables
variable "project_id" {
  description = "The Google Cloud project identifier"
  type = string
  default = "your-project-id <--"
}

variable "google_application_credentials" {
  description = "The path to the Google Cloud service account credentials"
  type = string
  default = "/path/to/service/account/key.json <--"
}

variable "environment" {
  description = "The environment type"
  type = string
  default = "dev"
}

variable "region" {
  description = "The Google Cloud region where the resources will be deployed"
  type = string
  default = "us-west1"
}

variable "zone" {
  description = "The Google Cloud zone where the resources will be deployed"
  type = string
  default = "us-west1-c"
}

variable "data_location" {
  description = "The location for storing data, including Google Cloud Storage buckets and Cloud SQL backups"
  type = string
  default = "us"
}

variable "database"{
  description = <<EOF
tier: The machine tier to use for the Observatory Platform Cloud SQL database.
backup_start_time: The time for Cloud SQL database backups to start in HH:MM format".
EOF
  type = object({
    tier = string
    backup_start_time = string
  })
  default = {
    tier = "db-custom-4-15360"
    backup_start_time = "'23:00'"
  }
}


variable "airflow_main"{
  description = <<EOF
machine_type: The machine type for the Airflow Main virtual machine.
disk_size: The disk size for the Airflow Main virtual machine in GB.
disk_type: The disk type for the Airflow Main virtual machine.
EOF
  type = object({
    machine_type = string
    disk_size = number
    disk_type = string
  })
  default = {
    machine_type = "n2-standard-2"
    disk_size = 20
    disk_type = "pd-ssd"
  }
}

variable "airflow_worker"{
  description = <<EOF
machine_type: The machine type for the Airflow Worker virtual machine(s).
disk_size: The disk size for the Airflow Worker virtual machine(s) in GB.
disk_type: The disk type for the Airflow Worker virtual machine(s).
EOF
  type = object({
    machine_type = string
    disk_size = number
    disk_type = string
  })
  default = {
    machine_type = "n1-standard-2"
    disk_size = 20
    disk_type = "pd-standard"
  }
}

variable "airflow_worker_create"{
  description = "Determines whether the airflow worker VM is created or destroyed"
  type = bool
  default = false
}

# Airflow secret variables
variable "airflow_secrets"{
  description = <<EOF
fernet_key: The fernet key which is used to encrypt the secrets in the airflow database.
postgres_password: The password for the airflow postgres database user.
redis_password: The password for redis, which is used by Celery to send messages, e.g. task messages
airflow_ui_user_password: The password for the Apache Airflow UI's airflow user.
airflow_ui_user_email: The email for the Apache Airflow UI's airflow user.
EOF
  type = object({
    fernet_key = string
    postgres_password = string
    redis_password = string
    airflow_ui_user_password = string
    airflow_ui_user_email = string
  })
  default = {
    "fernet_key":"created by generate config",
    "postgres_password":"random_password <--",
    "redis_password":"random_password <--",
    "airflow_ui_user_password":"random_password <--",
    "airflow_ui_user_email":"your.email@somehost.com <--",
  }
}

variable "airflow_connections"{
  description = <<EOF
mag_releases_table: The mag_releases_table connection.
mag_snapshots_container: The mag_snapshots_container connection.
crossref: Contains the crossref API token.
terraform: Contains the terraform API token.
slack: Contains the webhook URL and webhook token.
EOF
  type = object({
    mag_releases_table = string
    mag_snapshots_container = string
    crossref = string
    terraform = string
    slack = string
  })
  default = {
    "mag_releases_table":"mysql://myname:mypassword@myhost.com <--",
    "mag_snapshots_container": "mysql://myname:mypassword@myhost.com <--",
    "crossref":"mysql://myname:mypassword@myhost.com <--",
    "terraform":"mysql://:terraform-token@ <--",
    "slack":"https://:T00000000%2FB00000000%2FXXXXXXXXXXXXXXXXXXXXXXXX@https%3A%2F%2Fhooks.slack.com%2Fservices <--"
  }
}

variable "airflow_variables"{
  description = "Additional Airflow variables that aren't interpolated from Terraform resources"
  type = map(string)
  default = {
    "example_name": "example_value <--"
  }
}