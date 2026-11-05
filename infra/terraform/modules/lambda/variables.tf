# modules/lambda/variables.tf

variable "environment" {
  description = "Deployment environment (dev, staging, prod)."
  type        = string
}

variable "tags" {
  description = "Tags applied to all resources in this module."
  type        = map(string)
  default     = {}
}

variable "role_arn" {
  description = "ARN of the extract Lambda's execution role (module.iam.extract_role_arn)."
  type        = string
}

variable "scripts_bucket" {
  description = "Name of the scripts bucket holding the Lambda zip."
  type        = string
}

variable "package_key" {
  description = "S3 key of the Lambda zip in the scripts bucket."
  type        = string
  default     = "lambda/lambdas.zip"
}

variable "package_path" {
  description = "Local path of the zip scripts/build_lambda.sh builds (build/lambdas.zip)"
  type        = string
}

variable "data_bucket" {
  description = "Name of the raw layer bucket the extract writes to (DATA_BUCKET)."
  type        = string
}

variable "db_host" {
  description = "Writer endpoint of the shop's Aurora cluster (DB_HOST)."
  type        = string
}

variable "db_name" {
  description = "Postgres database name (DB_NAME)."
  type        = string
  default     = "shop"
}

variable "db_user" {
  description = "Postgres user the extract logs in as with IAM auth (DB_USER)."
  type        = string
  default     = "simulator"
}

variable "subnet_ids" {
  description = "Subnets the extract runs in (the default VPC's subnets)."
  type        = list(string)
}

variable "security_group_ids" {
  description = "Security groups attached to the extract (shop-sim-lambda)."
  type        = list(string)
}

variable "schedule_expression" {
  description = "When the daily extract runs, in UTC."
  type        = string
  default     = "cron(30 0 * * ? *)"
}

variable "scheduler_role_arn" {
  description = "ARN of the role EventBridge Scheduler assumes to invoke the extract (module.iam.extract_scheduler_role_arn)."
  type        = string
}
