## permanent_resources
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

## composer
variable "google-credentials_"{
  type = string
}
variable "composer-prefix_"{
  type = string
}
variable "machine-type"{
  type = string
}
variable "composer-disk-size-gb"{
  default=100
}
variable "composer-status_"{
  type = string
}