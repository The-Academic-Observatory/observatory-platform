output "airflow_db_ip_address" {
  value = google_sql_database_instance.observatory_db_instance.private_ip_address
  description = "The private IP address of the Airflow Cloud SQL database."
}

output "airflow_main_vm_ip_address" {
  value = module.airflow_main_vm.private_ip_address
  description = "The private IP address of the Airflow Main VM."
}

output "airflow_worker_vm_ip_address" {
  value = try(module.airflow_worker_vm.private_ip_address, null)
  description = "The private IP address of the Airflow Worker VM."
}

output "airflow_main_vm_script" {
  value = module.airflow_main_vm.vm_rendered
  description = "Rendered template file"
  sensitive = true # explicitly mark as sensitive so it can be exported
}

output "airflow_worker_vm_script" {
  value = try(module.airflow_worker_vm.vm_rendered, null)
  description = "Rendered template file"
}

output "project_number" {
  value = data.google_project.project.number
}

output "default_transfer_service_account" {
  value = data.google_storage_transfer_project_service_account.default.email
}

output "vpc_connector_name" {
  value = google_vpc_access_connector.observatory_vpc_connector.name
  description = "The vpc connector name"
}

output "observatory_db_uri" {
  value = "postgresql://${google_sql_user.observatory_user.name}:${urlencode(var.observatory.postgres_password)}@${google_sql_database_instance.observatory_db_instance.private_ip_address}:5432/${google_sql_database.observatory_db.name}"
  description = "The observatory database uri"
  sensitive = true
}