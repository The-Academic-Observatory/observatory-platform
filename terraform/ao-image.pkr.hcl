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
    inline = ["mkdir -p /tmp/academic-observatory/dags"]
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
    source = "docker-compose.airflow-secrets.yml"
    destination = "/tmp/academic-observatory/docker-compose.airflow-secrets.yml"
  }

  provisioner "file" {
    source = "docker-compose.airflow-main.yml"
    destination = "/tmp/academic-observatory/docker-compose.airflow-main.yml"
  }

  provisioner "file" {
    source = "docker-compose.airflow-worker.yml"
    destination = "/tmp/academic-observatory/docker-compose.airflow-worker.yml"
  }

  provisioner "file" {
    source = "terraform/hello_world.py"
    destination = "/tmp/academic-observatory/dags/hello_world.py"
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