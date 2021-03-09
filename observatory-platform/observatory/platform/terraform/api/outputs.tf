output "api-domain-name" {
  value = google_endpoints_service.api.service_name
  description = "Custom domain name for the API"
}

output "api-gateway-url" {
  value = google_cloud_run_service.api_gateway.status[0].url
  description = "Cloud run gateway URL for the API"
}