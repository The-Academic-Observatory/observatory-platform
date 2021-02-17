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

output "observatory_api_endpoints_url" {
  value = module.observatory_api.endpoints-url
  description = "API endpoint URL"
}

output "observatory_api_backend_url" {
  value = module.observatory_api.backend-url
  description = "API backend URL"
}