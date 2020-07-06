resource "google_compute_instance" "airflow-worker" {
  count = var.status == "on" ? 1 : 0
  name = "airflow-worker-${var.environment}"
  machine_type = "n1-standard-1"
  zone = var.zone

  tags = [
    "foo",
    "bar"]

  boot_disk {
    initialize_params {
      image = "debian-cloud/debian-9"
    }
  }

  network_interface {
    network = "default"

    access_config {
      // Ephemeral IP
    }
  }
}