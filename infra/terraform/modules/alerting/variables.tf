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
