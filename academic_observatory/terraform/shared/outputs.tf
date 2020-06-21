#TODO output created project
//output "project" {
//  value = google_project.telescope-project.name
//}
output "google_credentials" {
  value = var.google-credentials_
  sensitive = true
}
output "github_token"{
  value = var.github-token_
  sensitive = true
}
output "github_bucket_prod"{
  value = google_storage_bucket.github-bucket-prod.url
}
output "github_bucket_dev"{
  value = google_storage_bucket.github-bucket-dev.url
}
output "project"{
  value = var.project_
}
output "region"{
  value = var.region_
}
output "zone"{
  value = var.zone_
}