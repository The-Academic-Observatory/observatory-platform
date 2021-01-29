
variable "environment" {
  description = "The environment type: develop, staging or production."
  type = string
}

variable "airflow" {
  description = <<EOF
The Apache Airflow settings for the Observatory Platform.

fernet_key: the Fernet key.
ui_user_password: the password for the Apache Airflow UI admin user.
ui_user_email: the email address for the Apache Airflow UI admin user.
EOF
  type = object({
    fernet_key = string
    secret_key = string
    ui_user_email = string
    ui_user_password = string
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
postgres_password: the Postgres SQL password.
EOF
  type = object({
    tier = string
    backup_start_time = string
    postgres_password = string
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
