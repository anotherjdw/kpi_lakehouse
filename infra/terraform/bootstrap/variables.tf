# variables.tf -- inputs for the kpi_lakehouse CI/CD bootstrap root.

variable "aws_region" {
  description = "AWS region for the bootstrap resources."
  type        = string
  default     = "eu-central-1"
}

variable "data_product" {
  description = "Name of the data product these CI/CD identities serve."
  type        = string
  default     = "kpi_lakehouse"
}

variable "github_repo" {
  description = "GitHub repository in 'owner/repo' form whose Actions workflows may assume these roles. No default -- supply it at apply, e.g. -var 'github_repo=my-org/kpi_lakehouse'."
  type        = string
}

variable "environment" {
  description = "Deployment environment; must match the GitHub Environment that gates the apply job and the ci.yml 'environment:' value."
  type        = string
  default     = "prod"
}

variable "tfstate_bucket" {
  description = "S3 bucket holding Terraform state for both the main stack and this bootstrap root."
  type        = string
  default     = "kpi-lakehouse-tfstate"
}

variable "scripts_bucket_name" {
  description = "Name of the scripts S3 bucket the deploy/scripts identities write to. Empty falls back to '<data_product>-<environment>-scripts', lower-cased with '_' turned into '-' (local.bucket_prefix), matching the main stack's naming."
  type        = string
  default     = ""
}
