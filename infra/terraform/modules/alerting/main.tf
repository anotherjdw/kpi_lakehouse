# The alert topic for this data product, and the EventBridge rule that reports a
# Glue run stopped by hand. Glue jobs publish their own failure and stop alerts to
# the topic from the run record (runtime/job_settings.py); the rule adds who
# stopped a run, which only the CloudTrail record of the BatchStopJobRun call
# holds. The email subscriptions' addresses come from a CI secret, never the repo.

locals {
  common_tags = merge(var.tags, {
    Environment = var.environment
    ManagedBy   = "terraform"
  })
}

resource "aws_sns_topic" "alerts" {
  name = "${var.name_prefix}-alerts"
  tags = local.common_tags
}

resource "aws_sns_topic_subscription" "email" {
  count     = length(var.alert_email)
  topic_arn = aws_sns_topic.alerts.arn
  protocol  = "email"
  endpoint  = var.alert_email[count.index]
}

# Replaces the topic's default policy. The Glue role publishes through its own IAM
# policy (modules/iam), so the only statement needed here is for EventBridge, and
# only from the stop rule.
data "aws_iam_policy_document" "alerts" {
  statement {
    sid       = "StopRulePublish"
    effect    = "Allow"
    actions   = ["sns:Publish"]
    resources = [aws_sns_topic.alerts.arn]
    principals {
      type        = "Service"
      identifiers = ["events.amazonaws.com"]
    }
    condition {
      test     = "ArnEquals"
      variable = "aws:SourceArn"
      values   = [aws_cloudwatch_event_rule.glue_run_stopped.arn]
    }
  }
}

resource "aws_sns_topic_policy" "alerts" {
  arn    = aws_sns_topic.alerts.arn
  policy = data.aws_iam_policy_document.alerts.json
}

# A BatchStopJobRun call on one of this data product's jobs, as CloudTrail records
# it. Needs the account's trail (bootstrap root). A run that hits its Glue timeout
# makes no such call, so only the job's own STOPPED alert reports it.
resource "aws_cloudwatch_event_rule" "glue_run_stopped" {
  name        = "${var.name_prefix}-glue-run-stopped"
  description = "A run of a ${var.name_prefix} Glue job stopped through BatchStopJobRun."

  event_pattern = jsonencode({
    source        = ["aws.glue"]
    "detail-type" = ["AWS API Call via CloudTrail"]
    detail = {
      eventSource = ["glue.amazonaws.com"]
      eventName   = ["BatchStopJobRun"]
      requestParameters = {
        jobName = [{ prefix = "${var.name_prefix}-" }]
      }
    }
  })

  tags = local.common_tags
}

# The message is a JSON object, not a sentence: EventBridge inserts an array
# (the run IDs) into a template unquoted and unescaped, which only an object
# template keeps valid.
resource "aws_cloudwatch_event_target" "glue_run_stopped" {
  rule = aws_cloudwatch_event_rule.glue_run_stopped.name
  arn  = aws_sns_topic.alerts.arn

  input_transformer {
    input_paths = {
      job        = "$.detail.requestParameters.jobName"
      run_ids    = "$.detail.requestParameters.jobRunIds"
      stopped_by = "$.detail.userIdentity.arn"
      time       = "$.detail.eventTime"
      source_ip  = "$.detail.sourceIPAddress"
    }
    input_template = <<-TEMPLATE
      {
        "alert": "Glue run stopped by hand",
        "job": <job>,
        "run_ids": <run_ids>,
        "stopped_by": <stopped_by>,
        "time": <time>,
        "source_ip": <source_ip>
      }
    TEMPLATE
  }
}
