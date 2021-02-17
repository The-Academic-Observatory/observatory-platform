output "endpoints-url" {
  value = google_cloud_run_service.endpoints-cloud-run.status[0].url
  description = "API endpoint URL"
}

output "backend-url" {
  value = google_cloud_run_service.backend.status[0].url
  description = "API backend URL"
}