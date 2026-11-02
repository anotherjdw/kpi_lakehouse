# modules/lambda/outputs.tf

output "extract_function_name" {
  description = "Name of the extract Lambda function."
  value       = aws_lambda_function.extract.function_name
}

output "extract_function_arn" {
  description = "ARN of the extract Lambda function."
  value       = aws_lambda_function.extract.arn
}

output "extract_schedule_arn" {
  description = "ARN of the daily extract schedule."
  value       = aws_scheduler_schedule.extract_daily.arn
}
