terraform {
  required_providers {
    google = {
      source = "hashicorp/google"
    }
    random = {
      source = "hashicorp/random"
    }
    template = {
      source = "hashicorp/template"
    }
  }
  required_version = ">= 0.13.5"
}
