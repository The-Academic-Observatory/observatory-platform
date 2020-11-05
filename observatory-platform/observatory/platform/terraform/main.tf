########################################################################################################################
# Configure Google Cloud Provider
########################################################################################################################
terraform {
  backend "remote" {
    workspaces {
      prefix = "observatory-"
    }
  }
}

provider "google" {
  credentials = var.google_cloud.credentials
  project = var.google_cloud.project_id
  region = var.google_cloud.region
  zone = var.google_cloud.zone
}

data "google_project" "project" {
  project_id = var.google_cloud.project_id
  depends_on = [google_project_service.cloud_resource_manager]
}

data "google_compute_default_service_account" "default" {
  depends_on = [google_project_service.compute_engine, google_project_service.services]
}

data "google_storage_transfer_project_service_account" "default" {
  depends_on = [google_project_service.services]
}
locals {
  compute_service_account_email = data.google_compute_default_service_account.default.email
  transfer_service_account_email = data.google_storage_transfer_project_service_account.default.email
}

########################################################################################################################
# Terraform Cloud Environment Variable (https://www.terraform.io/docs/cloud/run/run-environment.html)
########################################################################################################################

variable "TFC_WORKSPACE_SLUG" {
  type = string
  default = "" # An error occurs when you are running TF backend other than Terraform Cloud
}

locals {
  organization = split("/", var.TFC_WORKSPACE_SLUG)[0]
  workspace_name = split("/", var.TFC_WORKSPACE_SLUG)[1]
  workspace_prefix =  "observatory-"
  workspace_suffix = trimprefix(local.workspace_name, local.workspace_prefix)
}

########################################################################################################################
# Enable google cloud APIs
########################################################################################################################

resource "google_project_service" "cloud_resource_manager" {
  project = var.google_cloud.project_id
  service = "cloudresourcemanager.googleapis.com"
  disable_dependent_services = true
}

# Can't disable dependent services, because of existing ao-image
resource "google_project_service" "compute_engine" {
  project = var.google_cloud.project_id
  service = "compute.googleapis.com"
  disable_on_destroy=false
  depends_on = [google_project_service.cloud_resource_manager]
}

resource "google_project_service" "services" {
  for_each = toset(["storagetransfer.googleapis.com", "iam.googleapis.com", "servicenetworking.googleapis.com",
"sqladmin.googleapis.com", "secretmanager.googleapis.com"])
  project = var.google_cloud.project_id
  service = each.key
  disable_dependent_services = true
  depends_on = [google_project_service.cloud_resource_manager]
}

########################################################################################################################
# Create a service account and add permissions
########################################################################################################################

resource "google_service_account" "observatory_service_account" {
  account_id   = var.google_cloud.project_id
  display_name = "Apache Airflow Service Account"
  description = "The Google Service Account used by Apache Airflow"
  depends_on = [google_project_service.services]
}

# Create service account key, save to Google Secrets Manager and give compute service account access to the secret
resource "google_service_account_key" "observatory_service_account_key" {
  service_account_id = google_service_account.observatory_service_account.name
}

# BigQuery admin
resource "google_project_iam_member" "observatory_service_account_bigquery_iam" {
  project = var.google_cloud.project_id
  role    = "roles/bigquery.admin"
  member  = "serviceAccount:${google_service_account.observatory_service_account.email}"
}

# Storage Transfer Admin
resource "google_project_iam_member" "observatory_service_account_storage_iam" {
  project = var.google_cloud.project_id
  role    = "roles/storagetransfer.admin"
  member  = "serviceAccount:${google_service_account.observatory_service_account.email}"
}

//module "google_application_credentials_secret" {
//  source = "./secret"
//  secret_id = "google_application_credentials"
//  secret_data = google_service_account_key.observatory_service_account_key.private_key
//  service_account_email = data.google_compute_default_service_account.default.email
//  depends_on = [google_project_service.services]
//}

########################################################################################################################
# Storage Buckets
########################################################################################################################
# Random id to prevent destroy of resources in keepers
resource "random_id" "buckets_protector" {
  count = var.environment == "production" ? 1 : 0
  byte_length = 8
  keepers = {
    download_bucket = google_storage_bucket.observatory_download_bucket.id
    transform_bucket = google_storage_bucket.observatory_transform_bucket.id
  }
  lifecycle {
    prevent_destroy = true
  }
}


# Bucket for storing downloaded files
resource "google_storage_bucket" "observatory_download_bucket" {
  name = "${var.google_cloud.project_id}-download"
  force_destroy = true
  location =  var.google_cloud.data_location
  project = var.google_cloud.project_id
  lifecycle_rule {
    condition {
      age = "31"
      matches_storage_class = ["STANDARD"]
    }
    action {
      type = "SetStorageClass"
      storage_class = "NEARLINE"
    }
  }
  lifecycle_rule {
    condition {
      age = "62"
      matches_storage_class = ["NEARLINE"]
    }
    action {
      type = "SetStorageClass"
      storage_class = "COLDLINE"
    }
  }
  lifecycle_rule {
    condition {
      age = "153"
      matches_storage_class = ["COLDLINE"]
    }
    action {
      type = "SetStorageClass"
      storage_class = "ARCHIVE"
    }
  }
}

# Permissions so that transfer service account can read / write files to bucket
resource "google_storage_bucket_iam_member" "observatory_download_bucket_transfer_service_account_legacy_bucket_reader" {
  bucket = google_storage_bucket.observatory_download_bucket.name
  role = "roles/storage.legacyBucketReader"
  member = "serviceAccount:${data.google_storage_transfer_project_service_account.default.email}"
}

resource "google_storage_bucket_iam_member" "observatory_download_bucket_transfer_service_account_object_creator" {
  bucket = google_storage_bucket.observatory_download_bucket.name
  role = "roles/storage.objectCreator"
  member = "serviceAccount:${data.google_storage_transfer_project_service_account.default.email}"
}

resource "google_storage_bucket_iam_member" "observatory_download_bucket_transfer_service_account_object_viewer" {
  bucket = google_storage_bucket.observatory_download_bucket.name
  role = "roles/storage.objectViewer"
  member = "serviceAccount:${data.google_storage_transfer_project_service_account.default.email}"
}

# Permissions so that Observatory Platform service account can read and write
resource "google_storage_bucket_iam_member" "observatory_download_bucket_observatory_service_account_legacy_bucket_reader" {
  bucket = google_storage_bucket.observatory_download_bucket.name
  role = "roles/storage.legacyBucketReader"
  member = "serviceAccount:${google_service_account.observatory_service_account.email}"
}

resource "google_storage_bucket_iam_member" "observatory_download_bucket_observatory_service_account_object_creator" {
  bucket = google_storage_bucket.observatory_download_bucket.name
  role = "roles/storage.objectCreator"
  member = "serviceAccount:${google_service_account.observatory_service_account.email}"
}

resource "google_storage_bucket_iam_member" "observatory_download_bucket_observatory_service_account_object_reader" {
  bucket = google_storage_bucket.observatory_download_bucket.name
  role = "roles/storage.objectViewer"
  member = "serviceAccount:${google_service_account.observatory_service_account.email}"
}

# Bucket for storing transformed files
resource "google_storage_bucket" "observatory_transform_bucket" {
  name = "${var.google_cloud.project_id}-transform"
  force_destroy = true
  location =  var.google_cloud.data_location
  project = var.google_cloud.project_id
  lifecycle_rule {
    condition {
      age = "31"
      matches_storage_class = ["STANDARD"]
    }
    action {
      type = "SetStorageClass"
      storage_class = "NEARLINE"
    }
  }
  lifecycle_rule {
    condition {
      age = "62"
      matches_storage_class = ["NEARLINE"]
    }
    action {
      type = "Delete"
    }
  }
}

# Permissions so that Observatory Platform service account can read and write
resource "google_storage_bucket_iam_member" "observatory_transform_bucket_observatory_service_account_legacy_bucket_reader" {
  bucket = google_storage_bucket.observatory_transform_bucket.name
  role = "roles/storage.legacyBucketReader"
  member = "serviceAccount:${google_service_account.observatory_service_account.email}"
}

resource "google_storage_bucket_iam_member" "observatory_transform_bucket_observatory_service_account_object_creator" {
  bucket = google_storage_bucket.observatory_transform_bucket.name
  role = "roles/storage.objectCreator"
  member = "serviceAccount:${google_service_account.observatory_service_account.email}"
}

resource "google_storage_bucket_iam_member" "observatory_transform_bucket_observatory_service_account_object_viewer" {
  bucket = google_storage_bucket.observatory_transform_bucket.name
  role = "roles/storage.objectViewer"
  member = "serviceAccount:${google_service_account.observatory_service_account.email}"
}

########################################################################################################################
# Observatory Platform VPC Network
########################################################################################################################

resource "google_compute_network" "observatory_network" {
  name = "ao-network"
  depends_on = [google_project_service.compute_engine]
}

data "google_compute_subnetwork" "observatory_subnetwork" {
  name = "ao-network"
  depends_on = [google_compute_network.observatory_network] # necessary to force reading of data
}

resource "google_compute_firewall" "allow_http" {
  name = "allow-http"
  description = "Allow HTTP ingress"
  network = google_compute_network.observatory_network.name

  allow {
    protocol = "tcp"
    ports = ["80"]
  }
  target_tags = ["http-server"]
  priority = 1000
}

resource "google_compute_firewall" "allow_https" {
  name = "allow-https"
  description = "Allow HTTPS ingress"
  network = google_compute_network.observatory_network.name

  allow {
    protocol = "tcp"
    ports = ["443"]
  }
  target_tags = ["https-server"]
  priority = 1000
}

resource "google_compute_firewall" "allow_internal" {
  name = "allow-internal"
  description = "Allow internal connections"
  network = google_compute_network.observatory_network.name
  source_ranges = ["10.128.0.0/9"]
  allow {
    protocol = "tcp"
    ports = ["0-65535"]
  }
  priority = 65534
}

resource "google_compute_firewall" "allow_ssh" {
  name = "allow-ssh"
  description = "Allow SSH from anywhere"
  network = google_compute_network.observatory_network.name

  allow {
    protocol = "tcp"
    ports = ["22"]
  }
  priority = 65534
}

########################################################################################################################
# Observatory Platform Cloud SQL database
########################################################################################################################

resource "google_compute_global_address" "airflow_db_private_ip" {
  name = "airflow-db-private-ip"
  purpose = "VPC_PEERING"
  address_type = "INTERNAL"
  prefix_length = 16
  network = google_compute_network.observatory_network.id
}

resource "google_service_networking_connection" "private_vpc_connection" {
  network = google_compute_network.observatory_network.id
  service = "servicenetworking.googleapis.com"
  reserved_peering_ranges = [google_compute_global_address.airflow_db_private_ip.name]
  depends_on = [google_project_service.services]
}

resource "random_id" "database_protector" {
  count = var.environment == "production" ? 1 : 0
  byte_length = 8
  keepers = {
    observatory_db_instance = google_sql_database_instance.observatory_db_instance.id
    airflow_db = google_sql_database.airflow_db.id
    users = google_sql_user.users.id
  }
  lifecycle {
    prevent_destroy = true
  }
}

resource "random_id" "airflow_db_name_suffix" {
  byte_length = 4
}

resource "google_sql_database_instance" "observatory_db_instance" {
  name = var.environment == "production" ? "observatory-db-instance" : "observatory-db-instance-${random_id.airflow_db_name_suffix.hex}"
  database_version = "POSTGRES_12"
  region = var.google_cloud.region
  deletion_protection = false

  depends_on = [google_service_networking_connection.private_vpc_connection, google_project_service.services]
  settings {
    tier = var.cloud_sql_database.tier
    ip_configuration {
      ipv4_enabled = false
      private_network = google_compute_network.observatory_network.id
    }
    backup_configuration {
      binary_log_enabled = false
      enabled = true
      location = var.google_cloud.data_location
      start_time = var.cloud_sql_database.backup_start_time
    }
  }
}

resource "google_sql_database" "airflow_db" {
  name = "airflow"
  depends_on = [google_sql_database_instance.observatory_db_instance]
  instance = google_sql_database_instance.observatory_db_instance.name
}

resource "google_sql_user" "users" {
  name = "airflow"
  instance = google_sql_database_instance.observatory_db_instance.name
  password = var.cloud_sql_database.postgres_password
}

########################################################################################################################
# User defined Apache Airflow variables stored as Google Cloud Secrets
########################################################################################################################

module "airflow_variables"{
  for_each = var.airflow_variables
  source = "./secret"
  secret_id = "airflow-variables-${each.key}"
  secret_data = each.value
  service_account_email = google_service_account.observatory_service_account.email
  depends_on = [google_project_service.services]
}

########################################################################################################################
# User defined Apache Airflow connections stored as Google Cloud Secrets
########################################################################################################################

module "airflow_connections"{
  for_each = var.airflow_connections
  source = "./secret"
  secret_id = "airflow-connections-${each.key}"
  secret_data = each.value
  service_account_email = google_service_account.observatory_service_account.email
  depends_on = [google_project_service.services]
}


########################################################################################################################
# Google Cloud Secrets required for the VMs
########################################################################################################################

locals {
  google_cloud_secrets = {
    airflow_ui_user_email=var.airflow.ui_user_email,
    airflow_ui_user_password=var.airflow.ui_user_password,
    fernet_key=var.airflow.fernet_key,
    google_application_credentials=var.google_cloud.credentials,
    postgres_password=var.cloud_sql_database.postgres_password
  }
}

module "google_cloud_secrets" {
  for_each = local.google_cloud_secrets
  source = "./secret"
  secret_id = each.key
  secret_data = contains(["postgres_password", "redis_password"], each.key) ? urlencode(each.value) : each.value
  service_account_email = data.google_compute_default_service_account.default.email
  depends_on = [google_project_service.services]
}

########################################################################################################################
# Airflow variables required for the VMs that will be exported as environment variables
########################################################################################################################

locals {
  airflow_variables = merge({
    project_id = var.google_cloud.project_id
    data_location = var.google_cloud.data_location
    download_bucket = google_storage_bucket.observatory_download_bucket.name
    transform_bucket = google_storage_bucket.observatory_transform_bucket.name
    terraform_organization = local.organization
    terraform_workspace_prefix = local.workspace_prefix
    environment = local.workspace_suffix
  }, var.airflow_variables)

  metadata_variables = {
    project_id = var.google_cloud.project_id
    postgres_hostname = google_sql_database_instance.observatory_db_instance.private_ip_address
    redis_hostname = module.airflow_main_vm.private_ip_address
    data_location = var.google_cloud.data_location
    download_bucket = google_storage_bucket.observatory_download_bucket.name
    transform_bucket = google_storage_bucket.observatory_transform_bucket.name
    terraform_organization =  local.organization
    terraform_workspace_prefix = local.workspace_prefix
    environment = local.workspace_suffix
    airflow_variables = local.airflow_variables
  }
}

########################################################################################################################
# Observatory Platform Main VM
########################################################################################################################

# Compute Image shared by both VMs
data "google_compute_image" "observatory_image" {
  name = "observatory-image"
  depends_on = [google_project_service.compute_engine]
}

module "airflow_main_vm" {
  source = "./vm"
  name = "airflow-main-vm"
  depends_on = [
    google_sql_database_instance.observatory_db_instance,
    module.google_cloud_secrets,
    module.airflow_variables,
    module.airflow_connections
  ]
  network = google_compute_network.observatory_network
  subnetwork = data.google_compute_subnetwork.observatory_subnetwork
  image = data.google_compute_image.observatory_image
  machine_type = var.airflow_main_vm.machine_type
  disk_size = var.airflow_main_vm.disk_size
  disk_type = var.airflow_main_vm.disk_type
  region = var.google_cloud.region
  service_account_email = local.compute_service_account_email
  startup_script_path = "./startup-main.tpl"
  metadata_variables = local.metadata_variables
}

########################################################################################################################
# Observatory Platform Worker VM
########################################################################################################################

module "airflow_worker_vm" {
  count = var.airflow_worker_vm.create == true ? 1 : 0
  source = "./vm"
  name = "airflow-worker-vm"
  depends_on = [module.airflow_main_vm]
  network = google_compute_network.observatory_network
  subnetwork = data.google_compute_subnetwork.observatory_subnetwork
  image = data.google_compute_image.observatory_image
  machine_type = var.airflow_worker_vm.machine_type
  disk_size = var.airflow_worker_vm.disk_size
  disk_type = var.airflow_worker_vm.disk_type
  region = var.google_cloud.region
  service_account_email = local.compute_service_account_email
  startup_script_path = "./startup-worker.tpl"
  metadata_variables = local.metadata_variables
}