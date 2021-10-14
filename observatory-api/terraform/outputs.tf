output "api-domain-name" {
  value       = module.api.api-domain-name
  description = "Custom domain name for the API"
  sensitive   = true
}

output "api-gateway-url" {
  value       = module.api.api-gateway-url
  description = "Cloud run gateway URL for the API"
  sensitive   = true
}