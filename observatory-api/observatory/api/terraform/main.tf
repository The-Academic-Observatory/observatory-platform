########################################################################################################################
# Configure Google Cloud Provider
########################################################################################################################

terraform {
  backend "remote" {
    workspaces {
      prefix = "observatory-"
    }
  }
  required_providers {
    google = {
      source  = "hashicorp/google"
      version = "~> 3.85.0"
    }
  }
}

provider "google" {
  credentials = var.google_cloud.credentials
  project     = var.google_cloud.project_id
  region      = var.google_cloud.region
  zone        = var.google_cloud.zone
}

data "terraform_remote_state" "observatory" {
  backend = "remote"
  config = {
    organization = var.observatory_api.observatory_organization
    workspaces = {
      name = var.observatory_api.observatory_workspace
    }
  }
}

locals {
  vpc_connector_name = try(data.terraform_remote_state.observatory.outputs.vpc_connector_name, null)
  observatory_db_uri = try(data.terraform_remote_state.observatory.outputs.observatory_db_uri, null)
}


module "api" {
  source  = "The-Academic-Observatory/api/google"
  version = "0.0.2"
  api = var.api
  environment = var.environment
  google_cloud = var.google_cloud
  observatory_api = {
    "create" = true,
    "vpc_connector_name": local.vpc_connector_name,
    "observatory_db_uri": local.observatory_db_uri}
}