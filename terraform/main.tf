########################################################################################################################
# Configure Google Cloud Provider
########################################################################################################################

provider "google" {
  credentials = file(var.credentials_file)
  project = var.project_id
  region = var.region
  zone = var.zone
}

data "google_project" "project" {
  project_id = var.project_id
}

data "google_compute_default_service_account" "default" {
}

data "google_storage_transfer_project_service_account" "default" {
}

locals {
  compute_service_account_email = data.google_compute_default_service_account.default.email
  transfer_service_account_email = data.google_storage_transfer_project_service_account.default.email
}

########################################################################################################################
# Create a service account and add permissions
########################################################################################################################

resource "google_service_account" "ao_service_account" {
  account_id   = "${var.project_id}-airflow"
  display_name = "Apache Airflow Service Account"
  description = "The Google Service Account used by Apache Airflow"
}

# Create service account key, save to Google Secrets Manager and give compute service account access to the secret
resource "google_service_account_key" "ao_service_account_key" {
  service_account_id = google_service_account.ao_service_account.name
}

module "google_application_credentials_secret" {
  source = "./secret"
  secret_id = "google_application_credentials"
  secret_data = google_service_account_key.ao_service_account_key.private_key
  service_account_email = data.google_compute_default_service_account.default.email
}

data "google_iam_policy" "ao_service_account_permissions" {
  binding {
    role = "roles/bigquery.admin"

    members = [
      "serviceAccount:${google_service_account.ao_service_account.email}"
    ]
  }

  binding {
    role = "roles/storagetransfer.admin"

    members = [
      "serviceAccount:${google_service_account.ao_service_account.email}"
    ]
  }
}

########################################################################################################################
# Storage Buckets
########################################################################################################################

# Bucket for storing downloaded files
resource "random_id" "ao_download_bucket_suffix" {
  byte_length = 8
}

resource "google_storage_bucket" "ao_download_bucket" {
  name = "${var.project_id}-download-${random_id.ao_download_bucket_suffix.hex}"
  force_destroy = true
  location =  var.data_location
  project = var.project_id
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
resource "google_storage_bucket_iam_member" "ao_download_bucket_transfer_service_account_legacy_bucket_reader" {
  bucket = google_storage_bucket.ao_download_bucket.name
  role = "roles/storage.legacyBucketReader"
  member = "serviceAccount:${data.google_storage_transfer_project_service_account.default.email}"
}

resource "google_storage_bucket_iam_member" "ao_download_bucket_transfer_service_account_object_creator" {
  bucket = google_storage_bucket.ao_download_bucket.name
  role = "roles/storage.objectCreator"
  member = "serviceAccount:${data.google_storage_transfer_project_service_account.default.email}"
}

resource "google_storage_bucket_iam_member" "ao_download_bucket_transfer_service_account_object_viewer" {
  bucket = google_storage_bucket.ao_download_bucket.name
  role = "roles/storage.objectViewer"
  member = "serviceAccount:${data.google_storage_transfer_project_service_account.default.email}"
}

# Permissions so that Academic Observatory service account can read and write
resource "google_storage_bucket_iam_member" "ao_download_bucket_ao_service_account_legacy_bucket_reader" {
  bucket = google_storage_bucket.ao_download_bucket.name
  role = "roles/storage.legacyBucketReader"
  member = "serviceAccount:${google_service_account.ao_service_account.email}"
}

resource "google_storage_bucket_iam_member" "ao_download_bucket_ao_service_account_object_creator" {
  bucket = google_storage_bucket.ao_download_bucket.name
  role = "roles/storage.objectCreator"
  member = "serviceAccount:${google_service_account.ao_service_account.email}"
}

resource "google_storage_bucket_iam_member" "ao_download_bucket_ao_service_account_object_reader" {
  bucket = google_storage_bucket.ao_download_bucket.name
  role = "roles/storage.objectViewer"
  member = "serviceAccount:${google_service_account.ao_service_account.email}"
}

# Bucket for storing transformed files
resource "random_id" "ao_transform_bucket_suffix" {
  byte_length = 8
}

resource "google_storage_bucket" "ao_transform_bucket" {
  name = "${var.project_id}-transform-${random_id.ao_transform_bucket_suffix.hex}"
  force_destroy = true
  location =  var.data_location
  project = var.project_id
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

# Permissions so that Academic Observatory service account can read and write
resource "google_storage_bucket_iam_member" "ao_transform_bucket_ao_service_account_legacy_bucket_reader" {
  bucket = google_storage_bucket.ao_transform_bucket.name
  role = "roles/storage.legacyBucketReader"
  member = "serviceAccount:${google_service_account.ao_service_account.email}"
}

resource "google_storage_bucket_iam_member" "ao_transform_bucket_ao_service_account_object_creator" {
  bucket = google_storage_bucket.ao_transform_bucket.name
  role = "roles/storage.objectCreator"
  member = "serviceAccount:${google_service_account.ao_service_account.email}"
}

resource "google_storage_bucket_iam_member" "ao_transform_bucket_ao_service_account_object_viewer" {
  bucket = google_storage_bucket.ao_transform_bucket.name
  role = "roles/storage.objectViewer"
  member = "serviceAccount:${google_service_account.ao_service_account.email}"
}

########################################################################################################################
# Academic Observatory VPC Network
########################################################################################################################

resource "google_compute_network" "ao_network" {
  name = "ao-network"
}

resource "google_compute_firewall" "allow_http" {
  name = "allow-http"
  description = "Allow HTTP ingress"
  network = google_compute_network.ao_network.name

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
  network = google_compute_network.ao_network.name

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
  network = google_compute_network.ao_network.name
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
  network = google_compute_network.ao_network.name

  allow {
    protocol = "tcp"
    ports = ["22"]
  }
  priority = 65534
}

########################################################################################################################
# Academic Observatory Cloud SQL database
########################################################################################################################

resource "google_compute_global_address" "airflow_db_private_ip" {
  name = "airflow-db-private-ip"
  purpose = "VPC_PEERING"
  address_type = "INTERNAL"
  prefix_length = 16
  network = google_compute_network.ao_network.id
}

resource "google_service_networking_connection" "private_vpc_connection" {
  network = google_compute_network.ao_network.id
  service = "servicenetworking.googleapis.com"
  reserved_peering_ranges = [google_compute_global_address.airflow_db_private_ip.name]
}

resource "random_id" "airflow_db_name_suffix" {
  byte_length = 4
}

resource "google_sql_database_instance" "ao_db_instance" {
  name = "ao-db-instance-${random_id.airflow_db_name_suffix.hex}"
  database_version = "POSTGRES_12"
  region = var.region
  depends_on = [google_service_networking_connection.private_vpc_connection]
  settings {
    tier = var.database_tier
    ip_configuration {
      ipv4_enabled = false
      private_network = google_compute_network.ao_network.id
    }
    backup_configuration {
      binary_log_enabled = false
      enabled = true
      location = var.data_location
      start_time = var.backup_start_time
    }
  }
}

resource "google_sql_database" "airflow_db" {
  name = "airflow"
  depends_on = [google_sql_database_instance.ao_db_instance]
  instance = google_sql_database_instance.ao_db_instance.name
}

resource "google_sql_user" "users" {
  name = "airflow"
  instance = google_sql_database_instance.ao_db_instance.name
  password = var.postgres_password
}

########################################################################################################################
# Airflow Secrets
########################################################################################################################
# These secrets are used by the VMs
# Fernet key
module "fernet_key_secret" {
  source = "./secret"
  secret_id = "fernet_key"
  secret_data = var.fernet_key
  service_account_email = local.compute_service_account_email
}

# Postgres password
module "postgres_password_secret" {
  source = "./secret"
  secret_id = "postgres_password"
  secret_data = urlencode(var.postgres_password)
  service_account_email = local.compute_service_account_email
}

# Airflow UI airflow user password
module "airflow_ui_user_password_secret" {
  source = "./secret"
  secret_id = "airflow_ui_user_password"
  secret_data = var.airflow_ui_user_password
  service_account_email = local.compute_service_account_email
}

# Redis password
module "redis_password_secret" {
  source = "./secret"
  secret_id = "redis_password"
  secret_data = var.redis_password
  service_account_email = local.compute_service_account_email
}

########################################################################################################################
# Airflow Connection Secrets
########################################################################################################################

# mag_releases_table connection
module "mag_releases_table_connection_secret" {
  source = "./secret"
  secret_id = "airflow-connections-mag_releases_table"
  secret_data = var.mag_releases_table_connection
  service_account_email = google_service_account.ao_service_account.email
}

# mag_snapshots_container connection
module "mag_snapshots_container_connection_secret" {
  source = "./secret"
  secret_id = "airflow-connections-mag_snapshots_container"
  secret_data = var.mag_snapshots_container_connection
  service_account_email = google_service_account.ao_service_account.email
}

# crossref connection
module "crossref_conn_secret" {
  source = "./secret"
  secret_id = "airflow-connections-crossref"
  secret_data = var.crossref_connection
  service_account_email = google_service_account.ao_service_account.email
}

########################################################################################################################
# Airflow Variables Secrets
########################################################################################################################

# Download bucket name
module "download_bucket_name_variable_secret" {
  source = "./secret"
  secret_id = "airflow-variables-download_bucket_name"
  secret_data = google_storage_bucket.ao_download_bucket.name
  service_account_email = google_service_account.ao_service_account.email
}

# Transform bucket name
module "transform_bucket_name_variable_secret" {
  source = "./secret"
  secret_id = "airflow-variables-transform_bucket_name"
  secret_data = google_storage_bucket.ao_transform_bucket.name
  service_account_email = google_service_account.ao_service_account.email
}

########################################################################################################################
# Academic Observatory Main VM
########################################################################################################################

# Compute Image shared by both VMs
data "google_compute_image" "ao_image" {
  name = "ao-image"
}

# Airflow Main VM
data "template_file" "airflow_main_vm_startup" {
  template = file("terraform/startup-main.tpl")
  vars = {
    project_id = var.project_id
    postgres_hostname = google_sql_database_instance.ao_db_instance.private_ip_address
  }
}

module "airflow_main_vm" {
  source = "./vm"
  name = "airflow-main-vm"
  depends_on = [
    google_sql_database_instance.ao_db_instance,
    module.google_application_credentials_secret,
    module.fernet_key_secret,
    module.postgres_password_secret,
    module.airflow_ui_user_password_secret,
    module.redis_password_secret
  ]
  network = google_compute_network.ao_network
  image = data.google_compute_image.ao_image
  region = var.region
  machine_type = var.airflow_main_machine_type
  service_account_email = local.compute_service_account_email
  metadata_startup_script = data.template_file.airflow_main_vm_startup.rendered
}

########################################################################################################################
# Academic Observatory Worker VM
########################################################################################################################

data "template_file" "airflow_worker_vm_startup" {
  template = file("terraform/startup-worker.tpl")
  vars = {
    project_id = var.project_id
    postgres_hostname = google_sql_database_instance.ao_db_instance.private_ip_address
    redis_hostname = module.airflow_main_vm.private_ip_address
  }
}

module "airflow_worker_vm" {
  source = "./vm"
  name = "airflow-worker-vm"
  depends_on = [module.airflow_main_vm]
  network = google_compute_network.ao_network
  image = data.google_compute_image.ao_image
  region = var.region
  machine_type = var.airflow_worker_machine_type
  service_account_email = local.compute_service_account_email
  metadata_startup_script = data.template_file.airflow_worker_vm_startup.rendered
}
