# composer
variable "google-credentials_"{
  type = string
}
variable "machine-type"{
  type = string
}
variable "composer-disk-size-gb"{
  default=100
}

# from root module
variable "github-token_"{
  type = string
}
variable "project_" {
  type = string
}
variable "region_" {
  type = string
}
variable "zone_" {
  type = string
}

# from workspace_name
variable "composer_name"{
  type = string
}
variable "github_bucket_path" {
  type = string
}
variable "status"{
  type = string
}
