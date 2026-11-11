variable "name_prefix" {
  description = "Prefix applied to the topic and rule names, and the prefix of the Glue job names the rule matches (typically '<data_product>-<environment>')."
  type        = string
}

variable "data_product" {
  description = "Data product name, the first part of the alarms' metric namespace '<data_product>/<environment>'."
  type        = string
}

variable "environment" {
  description = "Deployment environment (dev, staging, prod)."
  type        = string
}

variable "tags" {
  description = "Tags applied to all resources in this module."
  type        = map(string)
  default     = {}
}

variable "alert_email" {
  description = "Email addresses subscribed to the alert topic, one subscription each."
  type        = list(string)
  sensitive   = true
}

variable "missed_success_alarms" {
  description = "Jobs with a missed-success alarm, keyed by job name: the whole hours without a SUCCEEDED run record before it fires."
  type = map(object({
    missed_success_hours = number
  }))
  default = {}
}

variable "bucket_names" {
  description = "Bucket names keyed by layer (module.s3.bucket_names), for the sources' missed-landing alarms."
  type        = map(string)
  default     = {}
}

variable "ingestion_alarms" {
  description = "Ingestion Lambda functions the team owns, keyed by function name: a failure alarm, and a missed-success alarm after this many whole hours without a SUCCEEDED run record."
  type = map(object({
    missed_success_hours = number
  }))
  default = {}
}

variable "landing_alarms" {
  description = "Sources with a missed-landing alarm, keyed by dataset: the layer whose bucket holds the source's folder, and the whole hours without a new file there before it fires."
  type = map(object({
    layer                = string
    missed_landing_hours = number
  }))
  default = {}
}
