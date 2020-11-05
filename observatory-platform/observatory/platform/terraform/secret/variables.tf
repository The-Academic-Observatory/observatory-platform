variable "secret_id" {
  type = string
  description = "The id of the Google Secret Manager secret."
}

variable "secret_data" {
  type = string
  description = "The data for the Google Secret Manager secret."
}

variable "service_account_email" {
  type = string
  description = "The email of the service account which secret accessor rights will be granted to."
}