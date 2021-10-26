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

# Get info from the observatory workspace if this is given
data "terraform_remote_state" "observatory" {
  count   = var.api_type.type == "observatory_api" ? 1 : 0
  backend = "remote"
  config = {
    organization = var.api_type.observatory_organization
    workspaces = {
      name = var.api_type.observatory_workspace
    }
  }
}

# Get build info from a local file if no build_info is passed on
data "local_file" "image_tag" {
  count    = var.api.image_tag == "" ? 1 : 0
  filename = "./image_build.txt"
}

locals {
  # Set the build info, either from local file or from variable
  image_tag = try(data.local_file.image_tag[0].content, var.api.image_tag)

  # Set the environment variables for the Cloud Run backend
  env_vars = (
    var.api_type.type == "observatory_api" ?
    tomap({
      "OBSERVATORY_DB_URI" = data.terraform_remote_state.observatory[0].outputs.observatory_db_uri
    }) :
    tomap({
      "ES_HOST"    = var.api_type.elasticsearch_host,
      "ES_API_KEY" = var.api_type.elasticsearch_api_key,
    })
  )

  # Set the annotations for the cloud run backend.
  cloud_run_annotations = (
    var.api_type.type == "observatory_api" ?
    tomap({
      "autoscaling.knative.dev/maxScale"        = "10"
      "run.googleapis.com/vpc-access-egress"    = "private-ranges-only"
      "run.googleapis.com/vpc-access-connector" = "projects/${var.google_cloud.project_id}/locations/${var.google_cloud.region}/connectors/${data.terraform_remote_state.observatory[0].outputs.vpc_connector_name}"
    }) :
    tomap({
      "autoscaling.knative.dev/maxScale" = "10"
    })
  )
}


module "api" {
  source                = "The-Academic-Observatory/api/google"
  version               = "0.0.6"
  api                   = merge(var.api, { "image_tag" = local.image_tag })
  environment           = var.environment
  google_cloud          = var.google_cloud
  env_vars              = local.env_vars
  cloud_run_annotations = local.cloud_run_annotations
}
