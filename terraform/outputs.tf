
output "airflow_db_ip_address" {
  value = google_sql_database_instance.ao_db_instance.private_ip_address
  description = "The private IP address of the Airflow Cloud SQL database."
}

output "airflow_main_vm_script" {
  value = data.template_file.ao_main_vm_startup.rendered
  description = "Rendered template file"
}

output "project_number" {
  value = data.google_project.project.number
}

