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

  # The buckets of the layers that hold a source with a missed-landing alarm.
  landing_buckets = toset([for source in values(var.landing_alarms) : var.bucket_names[source.layer]])
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

# ---------------------------------------------------------------------------------------
# Ingestion the team owns (A5a), one set per function in var.ingestion_alarms. The function
# runs in a VPC with no route to SNS, so it can't publish; its log group can be read instead.
# ---------------------------------------------------------------------------------------

# A failure is a FAILED run record, Lambda's own line for a run cut off at its timeout
# (which prints no record), or Lambda's [ERROR] line for any unhandled error, including one
# raised before the handler runs. Any one of them counts.
resource "aws_cloudwatch_log_metric_filter" "ingestion_failures" {
  for_each = var.ingestion_alarms

  name           = "${var.name_prefix}-${each.key}-failures"
  log_group_name = "/aws/lambda/${each.key}"
  pattern = trimspace(<<-PATTERN
    ?"Task timed out" ?"[ERROR]" ?"\"status\": \"FAILED\""
  PATTERN
  )

  metric_transformation {
    name      = "${each.key}-failures"
    namespace = "${var.data_product}/${var.environment}"
    value     = "1"
  }
}

# Fires within a minute of the first failure. A minute with no failure has no data point,
# which isn't a failure.
resource "aws_cloudwatch_metric_alarm" "ingestion_failed" {
  for_each = var.ingestion_alarms

  alarm_name        = "${var.name_prefix}-${each.key}-failed"
  alarm_description = "${each.key} failed or timed out: its run record or Lambda's own line is in /aws/lambda/${each.key}."

  namespace           = "${var.data_product}/${var.environment}"
  metric_name         = aws_cloudwatch_log_metric_filter.ingestion_failures[each.key].metric_transformation[0].name
  statistic           = "Sum"
  period              = 60
  evaluation_periods  = 1
  threshold           = 1
  comparison_operator = "GreaterThanOrEqualToThreshold"
  treat_missing_data  = "notBreaching"

  alarm_actions = [aws_sns_topic.alerts.arn]
  ok_actions    = [aws_sns_topic.alerts.arn]

  tags = local.common_tags
}

# As the Glue jobs' missed-success alarms (A2), on the function's own log group.
resource "aws_cloudwatch_log_metric_filter" "ingestion_succeeded_runs" {
  for_each = var.ingestion_alarms

  name           = "${var.name_prefix}-${each.key}-succeeded-runs"
  log_group_name = "/aws/lambda/${each.key}"
  pattern = trimspace(<<-PATTERN
    "RUN_RECORD" "\"status\": \"SUCCEEDED\""
  PATTERN
  )

  metric_transformation {
    name      = "${each.key}-succeeded-runs"
    namespace = "${var.data_product}/${var.environment}"
    value     = "1"
  }
}

resource "aws_cloudwatch_metric_alarm" "ingestion_missed_success" {
  for_each = var.ingestion_alarms

  alarm_name        = "${var.name_prefix}-${each.key}-missed-success"
  alarm_description = "${each.key} has written no SUCCEEDED run record for ${each.value.missed_success_hours} hours."

  namespace           = "${var.data_product}/${var.environment}"
  metric_name         = aws_cloudwatch_log_metric_filter.ingestion_succeeded_runs[each.key].metric_transformation[0].name
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

# ---------------------------------------------------------------------------------------
# Source data the team doesn't own (A5b), one alarm per source in var.landing_alarms. S3
# reports every new object to EventBridge, one rule writes those events to a log group,
# and a filter per source counts the events for keys under its folder.
# ---------------------------------------------------------------------------------------

# Manages the whole notification configuration of each bucket; none was set before.
resource "aws_s3_bucket_notification" "landings" {
  for_each = local.landing_buckets

  bucket      = each.key
  eventbridge = true
}

resource "aws_cloudwatch_log_group" "landings" {
  name              = "/aws/events/${var.name_prefix}-landings"
  retention_in_days = 14
  tags              = local.common_tags
}

# Lets EventBridge write the events to the log group.
data "aws_iam_policy_document" "landings_log_group" {
  statement {
    sid       = "EventBridgeWrite"
    effect    = "Allow"
    actions   = ["logs:CreateLogStream", "logs:PutLogEvents"]
    resources = ["${aws_cloudwatch_log_group.landings.arn}:*"]
    principals {
      type        = "Service"
      identifiers = ["events.amazonaws.com", "delivery.logs.amazonaws.com"]
    }
  }
}

resource "aws_cloudwatch_log_resource_policy" "landings" {
  policy_name     = "${var.name_prefix}-landings"
  policy_document = data.aws_iam_policy_document.landings_log_group.json
}

resource "aws_cloudwatch_event_rule" "landings" {
  count = length(local.landing_buckets) > 0 ? 1 : 0

  name        = "${var.name_prefix}-landings"
  description = "A new object in a ${var.name_prefix} bucket that holds a source with a missed-landing alarm."

  event_pattern = jsonencode({
    source        = ["aws.s3"]
    "detail-type" = ["Object Created"]
    detail = {
      bucket = { name = sort(local.landing_buckets) }
    }
  })

  tags = local.common_tags
}

resource "aws_cloudwatch_event_target" "landings" {
  count = length(local.landing_buckets) > 0 ? 1 : 0

  rule = aws_cloudwatch_event_rule.landings[0].name
  arn  = aws_cloudwatch_log_group.landings.arn
}

# Any new object under the source's folder counts, whatever its partition. The events are
# pure JSON, so the filter reads the key directly; the slash keeps another dataset whose
# name starts the same way out.
resource "aws_cloudwatch_log_metric_filter" "landed_files" {
  for_each = var.landing_alarms

  name           = "${var.name_prefix}-${each.key}-landed-files"
  log_group_name = aws_cloudwatch_log_group.landings.name
  pattern        = "{ $.detail.object.key = \"${each.key}/*\" }"

  metric_transformation {
    name      = "${each.key}-landed-files"
    namespace = "${var.data_product}/${var.environment}"
    value     = "1"
  }
}

resource "aws_cloudwatch_metric_alarm" "missed_landing" {
  for_each = var.landing_alarms

  alarm_name        = "${var.name_prefix}-${each.key}-missed-landing"
  alarm_description = "No new file has landed under s3://${var.bucket_names[each.value.layer]}/${each.key}/ for ${each.value.missed_landing_hours} hours."

  namespace           = "${var.data_product}/${var.environment}"
  metric_name         = aws_cloudwatch_log_metric_filter.landed_files[each.key].metric_transformation[0].name
  statistic           = "Sum"
  period              = 3600
  evaluation_periods  = each.value.missed_landing_hours
  datapoints_to_alarm = each.value.missed_landing_hours
  threshold           = 1
  comparison_operator = "LessThanThreshold"
  treat_missing_data  = "breaching"

  alarm_actions = [aws_sns_topic.alerts.arn]
  ok_actions    = [aws_sns_topic.alerts.arn]

  tags = local.common_tags
}
