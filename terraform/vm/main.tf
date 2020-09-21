
resource "google_compute_address" "vm_private_ip" {
  name = "${var.name}-private-ip"
  address_type = "INTERNAL"
  subnetwork = var.subnetwork.self_link
  region = var.region
}

resource "google_compute_instance" "vm_instance" {
  name = var.name
  machine_type = var.machine_type
  allow_stopping_for_update = true

  boot_disk {
    initialize_params {
      image = var.image.self_link
      size = var.disk_size
      type = var.disk_type
    }
  }

  network_interface {
    network = var.network.name
    network_ip = google_compute_address.vm_private_ip.address
    subnetwork = var.subnetwork.name # Subnetwork should be specified for custom subnetmode network
    access_config {
    }
  }

  service_account {
    email = var.service_account_email
    scopes = ["cloud-platform"]
  }

  metadata_startup_script = templatefile(var.startup_script_path, {
    host_airflow_home = var.metadata_variables["host_airflow_home"],
    host_ao_home = var.metadata_variables["host_ao_home"],
    project_id = var.metadata_variables["project_id"],
    postgres_hostname = var.metadata_variables["postgres_hostname"],
    redis_hostname = var.metadata_variables["redis_hostname"],
    data_location = var.metadata_variables["data_location"],
    download_bucket_name = var.metadata_variables["download_bucket_name"],
    transform_bucket_name = var.metadata_variables["transform_bucket_name"],
    terraform_organization =  var.metadata_variables["terraform_organization"],
    terraform_prefix = var.metadata_variables["terraform_prefix"],
    environment = var.metadata_variables["environment"],
    airflow_variables = var.metadata_variables["airflow_variables"]}
  )
}