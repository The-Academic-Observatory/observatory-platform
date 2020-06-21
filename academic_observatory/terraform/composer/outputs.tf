output "airflow_ui" {
  value = try(google_composer_environment.cloud_composer[0].config.0.airflow_uri, "NA")
}
output "dags_bucket" {
  value = try(google_composer_environment.cloud_composer[0].config.0.dag_gcs_prefix, "NA")
}
output "credentials" {
  value = var.google-credentials_
  sensitive = true
}