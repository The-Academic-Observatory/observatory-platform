
resource "google_compute_address" "vm_private_ip" {
  name = "${var.name}-private-ip"
  address_type = "INTERNAL"
  subnetwork = var.network.id
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
    access_config {
    }
  }

  service_account {
    email = var.service_account_email
    scopes = ["cloud-platform"]
  }

  metadata_startup_script = var.metadata_startup_script
}