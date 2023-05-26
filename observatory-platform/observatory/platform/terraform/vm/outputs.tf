output "vm_rendered" {
  value = google_compute_instance.vm_instance.metadata_startup_script
}