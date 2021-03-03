output "gateway-url" {
  value = google_cloud_run_service.api_gateway.status[0].url
  description = "Cloud run gateway URL"
}

output "backend-url" {
  value = google_cloud_run_service.api_backend.status[0].url
  description = "Cloud run backend URL"
}