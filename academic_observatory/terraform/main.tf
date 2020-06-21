terraform {
  backend "remote" {
    hostname = "app.terraform.io"
    organization = "coki"

    workspaces {
      prefix = "ao-"
    }
  }
}
provider "google" {
  version = "3.5.0"
  credentials = var.google-credentials_
  #TODO use terraform_dir admin-project
  project = data.terraform_remote_state.shared.outputs.project
  region  = data.terraform_remote_state.shared.outputs.region
  zone    = data.terraform_remote_state.shared.outputs.zone
}
data "terraform_remote_state" "shared" {
  backend = "remote"

  config = {
    organization = "coki"

    workspaces = {
      name = "ao-shared"
    }
  }
}

variable "TFC_WORKSPACE_NAME" {
  type = string
  default = "" # An error occurs when you are running TF backend other than Terraform Cloud
}

locals {
  # If your backend is not Terraform Cloud, the value is terraform.workspace
  # otherwise the value retrieved is that of the TFC_WORKSPACE_NAME with trimprefix
  workspace_name = var.TFC_WORKSPACE_NAME != "" ? trimprefix(var.TFC_WORKSPACE_NAME, "ao-") : terraform.workspace
  dev = "dev"
  prod = "prod"
  shared = "shared"
  composer_name = "${var.composer-prefix_}${local.workspace_name}"
  github_bucket_path = local.workspace_name == local.dev || local.workspace_name == local.shared ? data.terraform_remote_state.shared.outputs.github_bucket_dev : data.terraform_remote_state.shared.outputs.github_bucket_dev
}


module "cloud_composer" {
  #TODO wait for cloud version 0.13.0
//  count = var.composer-status_ == "on" ? 1 : 0
  source = "./composer"
  status = var.composer-status_ == "on" ? "on" : "off"

  google-credentials_ = var.google-credentials_
  machine-type = var.machine-type
  composer-disk-size-gb = var.composer-disk-size-gb

  github-token_ = var.github-token_
  project_ = var.project_
  region_ = var.region_
  zone_ = var.zone_

  composer_name = local.composer_name
  github_bucket_path = local.github_bucket_path
}

