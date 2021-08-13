variable "environment" {
  description = "The environment type: develop, staging or production."
  type = string
}

variable "observatory" {
  description = <<EOF
The Observatory settings.

airflow_fernet_key: the Fernet key.
airflow_secret_key: the secret key used for the Flask Airflow Webserver.
airflow_ui_user_password: the password for the Apache Airflow UI admin user.
airflow_ui_user_email: the email address for the Apache Airflow UI admin user.
postgres_password: the Postgres SQL password.
EOF
  type = object({
    airflow_fernet_key = string
    airflow_secret_key = string
    airflow_ui_user_email = string
    airflow_ui_user_password = string
    postgres_password = string
  })
}

variable "google_cloud" {
  description = <<EOF
The Google Cloud settings for the Observatory Platform.

project_id: the Google Cloud project id.
credentials: the path to the Google Cloud credentials.
region: the Google Cloud region.
zone: the Google Cloud zone.
data_location: the data location for storing buckets.
EOF
  type = object({
    project_id = string
    credentials = string
    region = string
    zone = string
    data_location = string
  })
}

variable "cloud_sql_database" {
  description = <<EOF
The Google Cloud SQL database settings for the Observatory Platform.

tier: the database machine tier.
backup_start_time: the start time for backups in HH:MM format.
EOF
  type = object({
    tier = string
    backup_start_time = string
  })
}

variable "airflow_main_vm" {
  description = <<EOF
Settings for the main VM that runs the Apache Airflow scheduler and webserver.

machine_type: the type of Google Cloud virtual machine.
disk_size: the size of the disk in GB.
disk_type: the disk type.
create: whether to create the VM or not.
EOF
  type = object({
    machine_type = string
    disk_size = number
    disk_type = string
    create = bool
  })
}

variable "airflow_worker_vm" {
  description = <<EOF
Settings for the weekly on-demand VM that runs large tasks.

machine_type: the type of Google Cloud virtual machine.
disk_size: the size of the disk in GB.
disk_type: the disk type.
create: whether to create the VM or not.
EOF
  type = object({
    machine_type = string
    disk_size = number
    disk_type = string
    create = bool
  })
}

variable "airflow_variables" {
  description = "User defined Apache Airflow variables"
  type = map(string)
}

variable "airflow_connections" {
  description = "User defined Apache Airflow connections"
  type = map(string)
}

variable "elasticsearch" {
    description = <<EOF
Elasticsearch login information

api_key: The elasticsearch api-key
host: The address of the elasticsearch server
EOF
  type = object({
    api_key = string
    host = string
  })
}

variable "api" {
  description = <<EOF
Settings related to the Observatory API

domain_name: the custom domain name for the API, used for the google cloud endpoints service
subdomain: can be either 'project_id' or 'environment', used to determine a prefix for the domain_name
EOF
  type = object({
    domain_name = string
    subdomain = string
  })
}