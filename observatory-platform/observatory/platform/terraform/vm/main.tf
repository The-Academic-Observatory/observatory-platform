resource "google_compute_instance" "vm_instance" {
  name                      = var.name
  machine_type              = var.machine_type
  tags                      = ["allow-ssh", "allow-internal-airflow"]
  allow_stopping_for_update = true

  boot_disk {
    initialize_params {
      image = var.image.self_link
      size  = var.disk_size
      type  = var.disk_type
    }
  }

  network_interface {
    network    = var.network.name
    network_ip = var.internal_ip
    subnetwork = var.subnetwork.name # Subnetwork should be specified for custom subnetmode network
    access_config {
      nat_ip = var.external_ip
    }
  }

  service_account {
    email  = var.service_account_email
    scopes = ["cloud-platform"]
  }

  metadata_startup_script = templatefile(var.startup_script_path, {
    project_id        = var.metadata_variables.project_id,
    postgres_hostname = var.metadata_variables["postgres_hostname"],
    redis_hostname    = var.metadata_variables["redis_hostname"]
  })
}