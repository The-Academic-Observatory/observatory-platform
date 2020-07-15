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

  # Upload the Academic Observatory project files. Have to specify each file because the file provisioner doesn't
  # let you exclude files. Don't want folders e.g. venv or secrets to be uploaded.
  provisioner "shell" {
    inline = [
      "mkdir -p /tmp/academic-observatory/dags"]
  }

  provisioner "file" {
    source = "academic_observatory"
    destination = "/tmp/academic-observatory"
  }

  provisioner "file" {
    source = "tests"
    destination = "/tmp/academic-observatory"
  }

  provisioner "file" {
    source = "docker-compose.cloud.yml"
    destination = "/tmp/academic-observatory/docker-compose.cloud.yml"
  }

  provisioner "file" {
    source = "docker-compose.observatory.yml"
    destination = "/tmp/academic-observatory/docker-compose.observatory.yml"
  }

  provisioner "file" {
    source = "Dockerfile.observatory"
    destination = "/tmp/academic-observatory/Dockerfile.observatory"
  }

  provisioner "file" {
    source = "entrypoint-root.sh"
    destination = "/tmp/academic-observatory/entrypoint-root.sh"
  }

  provisioner "file" {
    source = "entrypoint-airflow.sh"
    destination = "/tmp/academic-observatory/entrypoint-airflow.sh"
  }

  provisioner "file" {
    source = "MANIFEST.in"
    destination = "/tmp/academic-observatory/MANIFEST.in"
  }

  provisioner "file" {
    source = "requirements.txt"
    destination = "/tmp/academic-observatory/requirements.txt"
  }

  provisioner "file" {
    source = "setup.cfg"
    destination = "/tmp/academic-observatory/setup.cfg"
  }

  provisioner "file" {
    source = "setup.py"
    destination = "/tmp/academic-observatory/setup.py"
  }

  # Run build script to install software on the image
  provisioner "shell" {
    script = "terraform/build.sh"
  }
}
