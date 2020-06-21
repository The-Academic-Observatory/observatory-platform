terraform {
  backend "remote" {
    hostname = "app.terraform.io"
    organization = "coki"

    workspaces {
      prefix = "ao-"
    }
  }
}
# credentials set with environment variable GOOGLE_CREDENTIALS=json key
provider "google" {
  version = "3.5.0"
  credentials = var.google-credentials_

  #TODO use terraform_dir admin-project
  project = var.project_
  region  = var.region_
  zone    = var.zone_
}


#TODO create project instead
//resource "google_project" "telescope-project"{
//  name = "telescope-project"
//  project_id = "telescope-project"
//  billing_acount = ?
//  org_id = "observatory.academy"
//}

data "google_project" "workflow-project" {
}

#TODO 3 buckets: dev, test, prod
//resource "google_storage_bucket" "github-bucket" {
//  # name convention of <project_name>-<project_number>-<bucket_name>
//  name     = "${data.google_project.workflow-project.name}-${tostring(data.google_project.workflow-project.number)}-dev"
//  project = google_project.telescope-project.project_id
//}

# Unfortunately repository dispatch also triggers master branch to sync.
resource "google_storage_bucket" "github-bucket-dev" {
  # name convention of <project_name>-<project_number>-<bucket_name>
  name     = "${data.google_project.workflow-project.name}-${tostring(data.google_project.workflow-project.number)}-github-dev"
  force_destroy = true
}
//resource "null_resource" "sync_github-dev" {
//  triggers = {
//    github_bucket_id = google_storage_bucket.github-bucket-dev.id
//  }
//  #TODO change github url to this repository once workflow is included
//  provisioner "local-exec" {
//    command = "curl -X POST https://api.github.com/repos/aroelo/sync_to_gcs/dispatches -H 'Accept: application/vnd.github.everest-preview+json' -H 'Authorization: token '\"$GITHUB_TOKEN\"'' --data '{\"event_type\": \"dev\"}'"
//    environment = {
//      GITHUB_TOKEN = var.github_token_
//    }
////    command = "git_root=$(git rev-parse --show-toplevel); gsutil -m rsync -R -c $${git_root}/academic_observatory ${google_storage_bucket.github-bucket-dev.url}"
////    command = "git_root='Users/aniekroelofs/PycharmProjects/academic-observatory'; gsutil -m rsync -R -c $${git_root}/academic_observatory ${google_storage_bucket.github-bucket-dev.url}"
//  }
//}

#TODO add project & update bucket name in github repo secrets when project is changed
resource "google_storage_bucket" "github-bucket-prod" {
  # name convention of <project_name>-<project_number>-<bucket_name>
  name     = "${data.google_project.workflow-project.name}-${tostring(data.google_project.workflow-project.number)}-github-prod"
  force_destroy = true
}
//resource "null_resource" "sync_github-prod" {
//  triggers = {
//    github_bucket_id = google_storage_bucket.github-bucket-prod.id
//  }
//  #TODO change github url to this repository
//  provisioner "local-exec" {
////    command = "git clone https://github.com/The-Academic-Observatory/academic-observatory.git ./tmpgithubdir; gsutil -m rsync -R -c ./tmpgithubdir ${google_storage_bucket.github-bucket-prod.url}; rm -rf ./tmpgithubdir"
//    command = "curl -X POST https://api.github.com/repos/aroelo/sync_to_gcs/dispatches -H 'Accept: application/vnd.github.everest-preview+json' -H 'Authorization: token '\"$GITHUB_TOKEN\"'' --data '{\"event_type\": \"prod\"}'"
//    environment = {
//      GITHUB_TOKEN = var.github_token
//    }
//  }
//}