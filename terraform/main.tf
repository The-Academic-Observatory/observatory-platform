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

locals {
  secret_accessor_role = "roles/secretmanager.secretAccessor"
  compute_service_account_email = "${data.google_project.project.number}-compute@developer.gserviceaccount.com"
  transfer_service_account_email = "project-${data.google_project.project.number}@storage-transfer-service.iam.gserviceaccount.com"
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
  reserved_peering_ranges = [
    google_compute_global_address.airflow_db_private_ip.name]
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
# Secrets
########################################################################################################################

# Fernet key
resource "google_secret_manager_secret" "fernet_key" {
  secret_id = "fernet_key"

  replication {
    automatic = true
  }
}

resource "google_secret_manager_secret_version" "fernet_key_version" {
  depends_on = [google_secret_manager_secret.fernet_key]
  secret = google_secret_manager_secret.fernet_key.id
  secret_data = var.fernet_key
}

resource "google_secret_manager_secret_iam_member" "fernet_key_member" {
  depends_on = [google_secret_manager_secret_version.fernet_key_version]
  project = google_secret_manager_secret.fernet_key.project
  secret_id = google_secret_manager_secret.fernet_key.secret_id
  role = local.secret_accessor_role
  member = "serviceAccount:${local.compute_service_account_email}"
}

# Postgres password
resource "google_secret_manager_secret" "postgres_password" {
  secret_id = "postgres_password"

  replication {
    automatic = true
  }
}

resource "google_secret_manager_secret_version" "postgres_password_version" {
  depends_on = [google_secret_manager_secret.postgres_password]
  secret = google_secret_manager_secret.postgres_password.id
  secret_data = urlencode(var.postgres_password)
}

resource "google_secret_manager_secret_iam_member" "postgres_password_member" {
  depends_on = [google_secret_manager_secret_version.postgres_password_version]
  project = google_secret_manager_secret.postgres_password.project
  secret_id = google_secret_manager_secret.postgres_password.secret_id
  role = local.secret_accessor_role
  member = "serviceAccount:${local.compute_service_account_email}"
}

# Airflow UI airflow user password
resource "google_secret_manager_secret" "airflow_ui_user_password" {
  secret_id = "airflow_ui_user_password"

  replication {
    automatic = true
  }
}

resource "google_secret_manager_secret_version" "airflow_ui_user_password_version" {
  depends_on = [google_secret_manager_secret.airflow_ui_user_password]
  secret = google_secret_manager_secret.airflow_ui_user_password.id
  secret_data = var.airflow_ui_user_password
}

resource "google_secret_manager_secret_iam_member" "airflow_ui_user_password_member" {
  depends_on = [google_secret_manager_secret_version.airflow_ui_user_password_version]
  project = google_secret_manager_secret.airflow_ui_user_password.project
  secret_id = google_secret_manager_secret.airflow_ui_user_password.secret_id
  role = local.secret_accessor_role
  member = "serviceAccount:${local.compute_service_account_email}"
}

# Redis password
resource "google_secret_manager_secret" "redis_password" {
  secret_id = "redis_password"

  replication {
    automatic = true
  }
}

resource "google_secret_manager_secret_version" "redis_password_version" {
  depends_on = [google_secret_manager_secret.redis_password]
  secret = google_secret_manager_secret.redis_password.id
  secret_data = var.redis_password
}

resource "google_secret_manager_secret_iam_member" "redis_password_member" {
  depends_on = [google_secret_manager_secret_version.redis_password_version]
  project = google_secret_manager_secret.redis_password.project
  secret_id = google_secret_manager_secret.redis_password.secret_id
  role = local.secret_accessor_role
  member = "serviceAccount:${local.compute_service_account_email}"
}

########################################################################################################################
# Academic Observatory Main VM
########################################################################################################################

# Compute Image
data "google_compute_image" "ao_image" {
  name = "ao-image"
}

# Static ip addresses
resource "google_compute_address" "airflow_main_vm_private_ip" {
  name = "airflow-main-vm-private-ip"
  address_type = "INTERNAL"
  subnetwork = google_compute_network.ao_network.id
  region = var.region
}

data "template_file" "ao_main_vm_startup" {
  template = file("terraform/startup-main.tpl")
  vars = {
    project_id = var.project_id
    postgres_hostname = google_sql_database_instance.ao_db_instance.private_ip_address
  }
}

resource "google_compute_instance" "airflow_main_vm_instance" {
  name = "airflow-main"
  machine_type = var.airflow_main_machine_type
  allow_stopping_for_update = true
  depends_on = [
    google_secret_manager_secret_iam_member.fernet_key_member,
    google_secret_manager_secret_iam_member.postgres_password_member,
    google_secret_manager_secret_iam_member.airflow_ui_user_password_member,
    google_secret_manager_secret_iam_member.redis_password_member,
    google_sql_database_instance.ao_db_instance]

  boot_disk {
    initialize_params {
      image = data.google_compute_image.ao_image.self_link
    }
  }
  network_interface {
    network = google_compute_network.ao_network.name
    network_ip = google_compute_address.airflow_main_vm_private_ip.address
    access_config {
    }
  }

  service_account {
    email = local.compute_service_account_email
    scopes = ["cloud-platform"]
  }

  metadata_startup_script = data.template_file.ao_main_vm_startup.rendered
}

########################################################################################################################
# Academic Observatory Worker VM
########################################################################################################################

resource "google_compute_address" "airflow_worker_vm_private_ip" {
  name = "airflow-worker-vm-private-ip"
  address_type = "INTERNAL"
  subnetwork = google_compute_network.ao_network.id
  region = var.region
}

data "template_file" "ao_worker_vm_startup" {
  template = file("terraform/startup-worker.tpl")
  vars = {
    project_id = var.project_id
    postgres_hostname = google_sql_database_instance.ao_db_instance.private_ip_address
    redis_hostname = google_compute_address.airflow_main_vm_private_ip.address
  }
}

resource "google_compute_instance" "airflow_worker_vm_instance" {
  name = "airflow-worker"
  machine_type = var.airflow_worker_machine_type
  allow_stopping_for_update = true
  depends_on = [
    google_secret_manager_secret_iam_member.fernet_key_member,
    google_secret_manager_secret_iam_member.postgres_password_member,
    google_secret_manager_secret_iam_member.airflow_ui_user_password_member,
    google_secret_manager_secret_iam_member.redis_password_member,
    google_sql_database_instance.ao_db_instance,
    google_compute_instance.airflow_main_vm_instance]

  boot_disk {
    initialize_params {
      image = data.google_compute_image.ao_image.self_link
    }
  }

  network_interface {
    network = google_compute_network.ao_network.name
    network_ip = google_compute_address.airflow_worker_vm_private_ip.address
    access_config {
    }
  }

  service_account {
    email = local.compute_service_account_email
    scopes = ["cloud-platform"]
  }

  metadata_startup_script = data.template_file.ao_worker_vm_startup.rendered
}