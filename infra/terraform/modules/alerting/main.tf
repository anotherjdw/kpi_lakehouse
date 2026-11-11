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

  # Where Glue's continuous logging writes the driver's stdout, run records included: one
  # log group shared by every Glue job in the account, one stream per run.
  glue_output_log_group = "/aws-glue/jobs/output"
}

data "aws_region" "current" {}
data "aws_caller_identity" "current" {}

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
# policy (modules/iam), so the statements here are for the services: EventBridge, only
# from the stop rule, and CloudWatch, only from this data product's alarms.
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

  statement {
    sid       = "AlarmsPublish"
    effect    = "Allow"
    actions   = ["sns:Publish"]
    resources = [aws_sns_topic.alerts.arn]
    principals {
      type        = "Service"
      identifiers = ["cloudwatch.amazonaws.com"]
    }
    condition {
      test     = "ArnLike"
      variable = "aws:SourceArn"
      values = [
        "arn:aws:cloudwatch:${data.aws_region.current.name}:${data.aws_caller_identity.current.account_id}:alarm:${var.name_prefix}-*",
      ]
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

# Missed-success alarms (A2), one per job in var.missed_success_alarms. A metric filter
# counts the job's SUCCEEDED run records, and the alarm fires when it has counted none for
# missed_success_hours whole hours. Every term is quoted, the record's own quotes included,
# so the Glue job name matches exactly and a longer name with the same start doesn't.
resource "aws_cloudwatch_log_metric_filter" "succeeded_runs" {
  for_each = var.missed_success_alarms

  name           = "${var.name_prefix}-${each.key}-succeeded-runs"
  log_group_name = local.glue_output_log_group
  pattern = trimspace(<<-PATTERN
    "RUN_RECORD" "\"status\": \"SUCCEEDED\"" "\"glue_job_name\": \"${var.name_prefix}-${each.key}\""
  PATTERN
  )

  metric_transformation {
    name      = "${each.key}-succeeded-runs"
    namespace = "${var.data_product}/${var.environment}"
    value     = "1"
  }
}

# No data point means no successful run that hour, so missing data counts as breaching.
# One message when it goes to ALARM, and one when a success brings it back to OK.
resource "aws_cloudwatch_metric_alarm" "missed_success" {
  for_each = var.missed_success_alarms

  alarm_name        = "${var.name_prefix}-${each.key}-missed-success"
  alarm_description = "${var.name_prefix}-${each.key} has written no SUCCEEDED run record for ${each.value.missed_success_hours} hours."

  namespace           = "${var.data_product}/${var.environment}"
  metric_name         = aws_cloudwatch_log_metric_filter.succeeded_runs[each.key].metric_transformation[0].name
  statistic           = "Sum"
  period              = 3600
  evaluation_periods  = each.value.missed_success_hours
  datapoints_to_alarm = each.value.missed_success_hours
  threshold           = 1
  comparison_operator = "LessThanThreshold"
  treat_missing_data  = "breaching"

  alarm_actions = [aws_sns_topic.alerts.arn]
  ok_actions    = [aws_sns_topic.alerts.arn]

  tags = local.common_tags
}
