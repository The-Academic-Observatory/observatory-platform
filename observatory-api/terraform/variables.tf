variable "environment" {
  description = "The environment type: develop, staging or production."
  type        = string
}

variable "google_cloud" {
  description = <<EOF
The Google Cloud settings for the Observatory Platform.

project_id: the Google Cloud project id.
credentials: the path to the Google Cloud credentials.
region: the Google Cloud region.
zone: the Google Cloud zone.
data_location: the data location for storing buckets.
EOF
  type = object({
    project_id    = string
    credentials   = string
    region        = string
    zone          = string
    data_location = string
  })
}

variable "api" {
  description = <<EOF
Settings related to the API

name: Name of the API project, e.g. academic-observatory or oaebu
//package_name: Local path to the Data API package, e.g. /path/to/academic_observatory_workflows_api
domain_name: The custom domain name for the API, used for the google cloud endpoints service
subdomain: Can be either 'project_id' or 'environment', used to determine a prefix for the domain_name
backend_image: The image URL that will be used for the Cloud Run backend.
gateway_image: The image URL that will be used for the Cloud Run gateway (endpoints service)
image_tag: The image tag that will be used for the Cloud Run backend. If the value is null, Terraform will get the image
tag from a local file "./image_build.txt".
EOF
  type = object({
    name            = string
    domain_name     = string
    subdomain       = string
    backend_image   = string
    gateway_image   = string
  })
}

variable "api_type" {
  description = <<EOF
Setting related to the specific api type, either the data api or observatory api.
The data api requires the observatory organization and workspace set, while the data api requires the elasticsearch
host and api key set.
EOF
  type = object({
    type                     = string
    observatory_organization = string
    observatory_workspace    = string
    elasticsearch_api_key    = string
    elasticsearch_host       = string
  })

  validation {
    condition     = var.api_type.type == "data_api" || var.api_type.type == "observatory_api"
    error_message = "The api type must either be 'data_api' or 'observatory_api'."
  }
  validation {
    condition = (
      var.api_type.type == "data_api" && var.api_type.elasticsearch_host != "" && var.api_type.elasticsearch_api_key != "" ||
      var.api_type.type == "observatory_api" && var.api_type.observatory_organization != "" && var.api_type.observatory_workspace != ""
    )
    error_message = "Elasticsearch host and api key can not be empty when the api type is set to 'data_api' and observatory organization and workspace can not be empty when the api type is set to 'observatory_api'.."
  }
}
