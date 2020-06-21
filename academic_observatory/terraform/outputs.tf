output "github_bucket" {
  value = local.github_bucket_path
}
output "airflow_ui" {
  value = module.cloud_composer.airflow_ui
}
output "dags_bucket" {
  value = module.cloud_composer.dags_bucket
}
output "google_credentials" {
  value = var.google-credentials_
  sensitive = true
}
