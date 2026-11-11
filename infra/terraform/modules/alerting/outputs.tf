output "topic_arn" {
  description = "ARN of the alert topic: the Glue jobs' --ALERT_TOPIC_ARN, and the resource of the Glue role's sns:Publish."
  value       = aws_sns_topic.alerts.arn
}
