# outputs.tf -- role ARNs to copy into the repository's Actions secrets.

output "plan_role_arn" {
  description = "ARN of the read-only Terraform plan role. Set repo secret AWS_PLAN_ROLE_ARN to this."
  value       = aws_iam_role.plan.arn
}

output "deploy_role_arn" {
  description = "ARN of the Terraform apply/deploy role. Set repo secret AWS_DEPLOY_ROLE_ARN to this."
  value       = aws_iam_role.deploy.arn
}

output "scripts_role_arn" {
  description = "ARN of the scripts-upload role. Set repo secret AWS_SCRIPTS_ROLE_ARN to this."
  value       = aws_iam_role.scripts.arn
}
