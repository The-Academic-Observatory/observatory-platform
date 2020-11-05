resource "google_secret_manager_secret" "secret" {
  secret_id = var.secret_id

  replication {
    automatic = true
  }
}

resource "google_secret_manager_secret_version" "secret_version" {
  depends_on = [google_secret_manager_secret.secret]
  secret = google_secret_manager_secret.secret.id
  secret_data = var.secret_data
}

resource "google_secret_manager_secret_iam_member" "secret_member" {
  depends_on = [google_secret_manager_secret_version.secret_version]
  project = google_secret_manager_secret.secret.project
  secret_id = google_secret_manager_secret.secret.secret_id
  role = "roles/secretmanager.secretAccessor"
  member = "serviceAccount:${var.service_account_email}"
}