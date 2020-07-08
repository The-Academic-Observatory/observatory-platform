
variable "name" {
  type = string
  description = ""
}

variable "image" {
  type = object({
    self_link=string
  })
  description = ""
}

variable "network" {
  type = object({
    id=string
    name=string
  })
  description = ""
}

variable "region" {
  type = string
  description = ""
}

variable "machine_type" {
  type = string
  description = ""
}

variable "service_account_email" {
  type = string
  description = ""
}

variable "metadata_startup_script" {
  type = string
  description = ""
}