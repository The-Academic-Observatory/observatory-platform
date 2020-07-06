terraform {
  backend "remote" {
    hostname = "app.terraform.io"
    organization = "coki"

    workspaces {
      prefix = "ao-"
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
  module = element(split("-", local.workspace_name), 0)
  environment = element(split("-", local.workspace_name), 1)
}

data "terraform_remote_state" "shared" {
  backend = "remote"

  config = {
    organization = "coki"

    workspaces = {
      name = "ao-shared-${local.environment}"
    }
  }
}

provider "google" {
  version = "3.5.0"
  credentials = var.google-credentials
  project = data.terraform_remote_state.shared.outputs.project
  region  = data.terraform_remote_state.shared.outputs.region
  zone    = data.terraform_remote_state.shared.outputs.zone
}

module "virtual_machine" {
  #TODO wait for cloud version 0.13.0
//  count = var.vm-status_ == "on" ? 1 : 0
  source = "./vm"
  status = var.vm-status == "on" ? "on" : "off"
  environment = local.environment

  zone    = data.terraform_remote_state.shared.outputs.zone
}