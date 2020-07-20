# Variables
variable "credentials_file" {
  type = string
}

variable "project_id" {
  type = string
}

variable "zone" {
  type = string
}

# Builder
source "googlecompute" "ao_builder" {
  account_file = var.credentials_file
  project_id = var.project_id
  ssh_username = "packer"
  source_image_family = "ubuntu-1804-lts"
  machine_type = "n1-standard-1"
  zone = var.zone
  image_name = "ao-image"
}

# Build commands
build {
  sources = ["source.googlecompute.ao_builder"]

  # Upload the Observatory Platform project files. Have to specify each file because the file provisioner doesn't
  # let you exclude files. Don't want folders e.g. venv or secrets to be uploaded.
  provisioner "shell" {
    inline = [
      "mkdir -p /tmp/observatory-platform/docs"]
  }

  provisioner "file" {
    source = "observatory_platform"
    destination = "/tmp/observatory-platform"
  }

  provisioner "file" {
    source = "tests"
    destination = "/tmp/observatory-platform"
  }

  provisioner "file" {
    source = "docker-compose.cloud.yml"
    destination = "/tmp/observatory-platform/docker-compose.cloud.yml"
  }

  provisioner "file" {
    source = "docker-compose.observatory.yml"
    destination = "/tmp/observatory-platform/docker-compose.observatory.yml"
  }

  provisioner "file" {
    source = "Dockerfile.observatory"
    destination = "/tmp/observatory-platform/Dockerfile.observatory"
  }

  provisioner "file" {
    source = "entrypoint-root.sh"
    destination = "/tmp/observatory-platform/entrypoint-root.sh"
  }

  provisioner "file" {
    source = "entrypoint-airflow.sh"
    destination = "/tmp/observatory-platform/entrypoint-airflow.sh"
  }

  provisioner "file" {
    source = "MANIFEST.in"
    destination = "/tmp/observatory-platform/MANIFEST.in"
  }

  provisioner "file" {
    source = "requirements.txt"
    destination = "/tmp/observatory-platform/requirements.txt"
  }

  provisioner "file" {
    source = "docs/requirements.txt"
    destination = "/tmp/observatory-platform/docs/requirements.txt"
  }

  provisioner "file" {
    source = "setup.cfg"
    destination = "/tmp/observatory-platform/setup.cfg"
  }

  provisioner "file" {
    source = "setup.py"
    destination = "/tmp/observatory-platform/setup.py"
  }

  # Run build script to install software on the image
  provisioner "shell" {
    script = "terraform/build.sh"
  }
}
