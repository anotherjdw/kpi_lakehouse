# Extract Lambda: Postgres (IAM auth) -> raw layer in S3, invoked once a day by EventBridge Scheduler

locals {
  common_tags = merge(var.tags, {
    Environment = var.environment
    ManagedBy   = "terraform"
  })
}

# -----------------------
# Extract Lambda function
# -----------------------

# Read the uploaded zip's version, so a new upload under the same key redeploys the function.
data "aws_s3_object" "package" {
  bucket = var.scripts_bucket
  key    = var.package_key
}

resource "aws_lambda_function" "extract" {
  function_name = "shop-sim-extract"
  role          = var.role_arn
  handler       = "shop_simulator.extract.handler"
  runtime       = "python3.12"
  architectures = ["x86_64"]
  memory_size   = 512
  timeout       = 300

  s3_bucket         = var.scripts_bucket
  s3_key            = var.package_key
  s3_object_version = data.aws_s3_object.package.version_id

  environment {
    variables = {
      DATA_BUCKET = var.data_bucket
      RAW_PREFIX  = "/"
      DB_HOST     = var.db_host
      DB_NAME     = var.db_name
      DB_USER     = var.db_user
    }
  }

  vpc_config {
    subnet_ids         = var.subnet_ids
    security_group_ids = var.security_group_ids
  }

  tags = local.common_tags
}

resource "aws_scheduler_schedule" "extract_daily" {
  name                         = "shop-sim-extract-daily"
  schedule_expression          = var.schedule_expression
  schedule_expression_timezone = "UTC"

  flexible_time_window {
    mode = "OFF"
  }

  target {
    arn      = aws_lambda_function.extract.arn
    role_arn = var.scheduler_role_arn
    input    = "{}"
  }
}
