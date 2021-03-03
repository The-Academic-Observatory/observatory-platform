########################################################################################################################
# Enable API services
########################################################################################################################

resource "google_project_service" "servicemanagement" {
  project = var.google_cloud.project_id
  service = "servicemanagement.googleapis.com"
  disable_on_destroy = false
}

resource "google_project_service" "servicecontrol" {
  project = var.google_cloud.project_id
  service = "servicecontrol.googleapis.com"
  disable_on_destroy = false
}

resource "google_project_service" "endpoints" {
  project = var.google_cloud.project_id
  service = "endpoints.googleapis.com"
  disable_on_destroy = false
}

# Enable service
resource "google_project_service" "api-project-service" {
  service = google_endpoints_service.api.service_name
  project = var.google_cloud.project_id
  depends_on = [google_endpoints_service.api]
  disable_on_destroy = false
}

########################################################################################################################
# Cloud Run backend for API
########################################################################################################################
resource "google_service_account" "api-backend_service_account" {
  account_id   = "api-backend"
  display_name = "API backend Service Account"
  description = "The Google Service Account used by the cloud run backend"
}

# Create elasticsearch secrets
module "elasticsearch-logins" {
  for_each = var.elasticsearch
  source = "../secret"
  secret_id = "elasticsearch-${each.key}"
  secret_data = each.value
  service_account_email = google_service_account.api-backend_service_account.email
}

resource "google_cloud_run_service" "api_backend" {
  name     = "api-backend"
  location = var.google_cloud.region

  template {
    spec {
      containers {
        image = "gcr.io/${var.google_cloud.project_id}/observatory-api"
        env {
          name = "ES_API_KEY"
          value = "sm://${var.google_cloud.project_id}/elasticsearch-api_key"
        }
        env {
          name = "ES_HOST"
          value = "sm://${var.google_cloud.project_id}/elasticsearch-host"
        }
      }
      service_account_name = google_service_account.api-backend_service_account.email
    }
  }

  traffic {
    percent         = 100
    latest_revision = true
  }
  depends_on = [module.elasticsearch-logins]
}

########################################################################################################################
# Endpoints service
########################################################################################################################
# Random id to use in endpoints hostname
resource "random_id" "endpoint-suffix" {
  byte_length = 8
}

locals {
  #TODO get "api.observatory.academy" from variable
  custom_domain =  var.environment == "production" ? "api.observatory.academy" : "${var.environment}.api.observatory.academy"
}

# Create/update endpoints configuration based on OpenAPI
resource "google_endpoints_service" "api" {
  project = var.google_cloud.project_id
  service_name = local.custom_domain
  openapi_config = templatefile("./openapi_endpoint.yml", {
    host = local.custom_domain
    backend_address = google_cloud_run_service.api_backend.status[0].url,
    query_parameters = ["id", "name", "published_year", "coordinates", "country", "country_code", "region",
                               "subregion", "access_type", "label", "status", "collaborator_coordinates",
                               "collaborator_country", "collaborator_country_code", "collaborator_id",
                               "collaborator_name", "collaborator_region", "collaborator_subregion", "field", "source",
                               "funder_country_code", "funder_name", "funder_sub_type", "funder_type", "journal",
                               "output_type", "publisher"]
  })
}

# Create service account used by Cloud Run
resource "google_service_account" "api-endpoint_service_account" {
  account_id = "api-endpoint"
  display_name = "API endpoint Service Account"
  description = "The Google Service Account used by the cloud run endpoint"
}


# Give permission to Cloud Run Endpoint service-account to access private Cloud Run backend
resource "google_project_iam_member" "api-endpoint_service_account_cloudrun_iam" {
  project = var.google_cloud.project_id
  role    = "roles/run.invoker"
  member  = "serviceAccount:${google_service_account.api-endpoint_service_account.email}"
}

# Give permission to Cloud Run Endpoint service-account to control service management
resource "google_project_iam_member" "api-endpoint_service_account_servicecontroller_iam" {
  project = var.google_cloud.project_id
  role    = "roles/servicemanagement.serviceController"
  member  = "serviceAccount:${google_service_account.api-endpoint_service_account.email}"
}

# Create/update Cloud Run service
resource "google_cloud_run_service" "api_gateway" {
  name     = "api-gateway"
  location = var.google_cloud.region
  project = var.google_cloud.project_id
  template {
    spec {
      containers {
        image = "gcr.io/endpoints-release/endpoints-runtime-serverless:2"
        env {
          name = "ENDPOINTS_SERVICE_NAME"
          value = google_endpoints_service.api.service_name
        }
      }
      service_account_name = google_service_account.api-endpoint_service_account.email
    }
  }
  depends_on = [google_endpoints_service.api, google_project_iam_member.api-endpoint_service_account_servicecontroller_iam]
}

# Create custom domain mapping for cloud run gateway
resource "google_cloud_run_domain_mapping" "default" {
  location = google_cloud_run_service.api_gateway.location
  name     = local.custom_domain

  metadata {
    namespace = var.google_cloud.project_id
  }

  spec {
    route_name = google_cloud_run_service.api_gateway.name
  }
}

# Create public access policy
data "google_iam_policy" "noauth" {
  binding {
    role = "roles/run.invoker"
    members = [
      "allUsers",
    ]
  }
}

# Enable public access policy on endpoints service (access is restricted with API key by openapi config)
resource "google_cloud_run_service_iam_policy" "noauth-endpoints" {
  location    = google_cloud_run_service.api_gateway.location
  project     = google_cloud_run_service.api_gateway.project
  service     = google_cloud_run_service.api_gateway.name
  policy_data = data.google_iam_policy.noauth.policy_data
}