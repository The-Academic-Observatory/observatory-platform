terraform {
  backend "remote" {
    hostname = "app.terraform.io"
    organization = "coki"

    workspaces {
      prefix = "ao-"
    }
  }
}
# credentials set with environment variable GOOGLE_CREDENTIALS=json key
provider "google" {
  version = "3.5.0"
  credentials = var.google-credentials

  project = var.project
  region  = var.region
  zone    = var.zone
}

variable "TFC_WORKSPACE_NAME" {
  type = string
  default = "" # An error occurs when you are running TF backend other than Terraform Cloud
}

locals {
  # If your backend is not Terraform Cloud, the value is terraform.workspace
  # otherwise the value retrieved is that of the TFC_WORKSPACE_NAME with trimprefix
  workspace_name = var.TFC_WORKSPACE_NAME != "" ? trimprefix(var.TFC_WORKSPACE_NAME, "ao-") : terraform.workspace
  module = element(split("-", local.workspace_name), 0)
  environment = element(split("-", local.workspace_name), 1)
}

resource "google_compute_instance" "airflow-main" {
  name = "airflow-main-${local.environment}"
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