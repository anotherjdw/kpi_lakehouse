# main.tf -- CI/CD identities for kpi_lakehouse, and adds the account's CloudTrail trail,
# provisioned as code. Creates the GitHub OIDC provider and three least-privilege roles the data
# product's GitHub Actions workflow assumes: plan (read-only), deploy
# (apply the main stack), and scripts (upload artifacts). See README.md.

data "aws_caller_identity" "current" {}

locals {
  name_prefix = "${var.data_product}-${var.environment}"

  # S3 bucket names allow only lowercase letters, digits and hyphens, so buckets use this form
  # of name_prefix, as in the main stack; IAM roles and Glue jobs keep name_prefix itself.
  bucket_prefix = replace(lower(local.name_prefix), "_", "-")

  # github_repo in GitHub's immutable subject format, repo:<owner>@<owner id>/<repo>@<repo id>,
  # with the ids as wildcards. Repositories use this format or the plain owner/repo one,
  # depending on their OIDC settings, so every trust policy accepts both.
  github_repo_immutable = "${split("/", var.github_repo)[0]}@*/${split("/", var.github_repo)[1]}@*"

  scripts_bucket = var.scripts_bucket_name != "" ? var.scripts_bucket_name : "${local.bucket_prefix}-scripts"

  # State objects for this data product (both the main stack's key and this
  # bootstrap root's key live under <data_product>/), including their
  # ".tflock" lock objects created by the S3-native backend lock.
  state_bucket_arn  = "arn:aws:s3:::${var.tfstate_bucket}"
  state_objects_arn = "arn:aws:s3:::${var.tfstate_bucket}/${var.data_product}/*"

  # Main-stack resources, scoped by the shared name prefix.
  account_id        = data.aws_caller_identity.current.account_id
  project_role      = "arn:aws:iam::${local.account_id}:role/${local.name_prefix}-*"
  project_policy    = "arn:aws:iam::${local.account_id}:policy/${local.name_prefix}-*"
  project_buckets   = "arn:aws:s3:::${local.bucket_prefix}-*"
  project_jobs      = "arn:aws:glue:*:${local.account_id}:job/${local.name_prefix}-*"
  project_triggers  = "arn:aws:glue:*:${local.account_id}:trigger/${local.name_prefix}-*"
  project_workflows = "arn:aws:glue:*:${local.account_id}:workflow/${local.name_prefix}-*"
  project_topics    = "arn:aws:sns:*:${local.account_id}:${local.name_prefix}-*"
  project_rules     = "arn:aws:events:*:${local.account_id}:rule/${local.name_prefix}-*"
  project_alarms    = "arn:aws:cloudwatch:*:${local.account_id}:alarm:${local.name_prefix}-*"

  # The log group Glue writes every job's driver stdout to, which the missed-success
  # metric filters read. It is shared by every Glue job in the account.
  glue_output_log_group = "arn:aws:logs:*:${local.account_id}:log-group:/aws-glue/jobs/output"

  # The extract's own log group, created by Lambda, which its alarms' metric filters read.
  extract_log_group = "arn:aws:logs:*:${local.account_id}:log-group:/aws/lambda/shop-sim-extract"

  # The log group EventBridge writes the raw bucket's new-object events to, for the
  # missed-landing alarms.
  landings_log_group = "arn:aws:logs:*:${local.account_id}:log-group:/aws/events/${local.name_prefix}-landings"

  # Catalog names hold no hyphens: a layer's database is named after its bucket with
  # "-" turned into "_" (modules/catalog in the main stack).
  catalog_prefix    = replace(local.bucket_prefix, "-", "_")
  project_catalog   = "arn:aws:glue:*:${local.account_id}:catalog"
  project_databases = "arn:aws:glue:*:${local.account_id}:database/${local.catalog_prefix}_*"
  project_tables    = "arn:aws:glue:*:${local.account_id}:table/${local.catalog_prefix}_*/*"
  project_functions = "arn:aws:glue:*:${local.account_id}:userDefinedFunction/${local.catalog_prefix}_*/*"

  # The account's CloudTrail trail and its bucket. The names hold no project prefix,
  # they are for the whole account, and the deploy role's bucket permissions (local.project_buckets)
  # must not reach its bucket.
  trail_name   = "management-events"
  trail_arn    = "arn:aws:cloudtrail:${var.aws_region}:${local.account_id}:trail/${local.trail_name}"
  trail_bucket = "cloudtrail-${local.account_id}-${var.aws_region}"
}

# ---------------------------------------------------------------------------
# GitHub OIDC provider
# ---------------------------------------------------------------------------

data "tls_certificate" "github" {
  url = "https://token.actions.githubusercontent.com/.well-known/openid-configuration"
}

resource "aws_iam_openid_connect_provider" "github" {
  url             = "https://token.actions.githubusercontent.com"
  client_id_list  = ["sts.amazonaws.com"]
  thumbprint_list = [data.tls_certificate.github.certificates[0].sha1_fingerprint]
}

# ---------------------------------------------------------------------------
# Trust policies -- who may assume each role, scoped to var.github_repo
# ---------------------------------------------------------------------------

data "aws_iam_policy_document" "plan_trust" {
  statement {
    effect  = "Allow"
    actions = ["sts:AssumeRoleWithWebIdentity"]

    principals {
      type        = "Federated"
      identifiers = [aws_iam_openid_connect_provider.github.arn]
    }

    condition {
      test     = "StringEquals"
      variable = "token.actions.githubusercontent.com:aud"
      values   = ["sts.amazonaws.com"]
    }

    condition {
      test     = "StringLike"
      variable = "token.actions.githubusercontent.com:sub"
      values = [
        "repo:${var.github_repo}:pull_request",
        "repo:${var.github_repo}:ref:refs/heads/main",
        "repo:${local.github_repo_immutable}:pull_request",
        "repo:${local.github_repo_immutable}:ref:refs/heads/main",
      ]
    }
  }
}

data "aws_iam_policy_document" "deploy_trust" {
  statement {
    effect  = "Allow"
    actions = ["sts:AssumeRoleWithWebIdentity"]

    principals {
      type        = "Federated"
      identifiers = [aws_iam_openid_connect_provider.github.arn]
    }

    condition {
      test     = "StringEquals"
      variable = "token.actions.githubusercontent.com:aud"
      values   = ["sts.amazonaws.com"]
    }

    # Only a job bound to the "${var.environment}" Environment can assume deploy.
    condition {
      test     = "StringLike"
      variable = "token.actions.githubusercontent.com:sub"
      values = [
        "repo:${var.github_repo}:environment:${var.environment}",
        "repo:${local.github_repo_immutable}:environment:${var.environment}",
      ]
    }
  }
}

data "aws_iam_policy_document" "scripts_trust" {
  statement {
    effect  = "Allow"
    actions = ["sts:AssumeRoleWithWebIdentity"]

    principals {
      type        = "Federated"
      identifiers = [aws_iam_openid_connect_provider.github.arn]
    }

    condition {
      test     = "StringEquals"
      variable = "token.actions.githubusercontent.com:aud"
      values   = ["sts.amazonaws.com"]
    }

    condition {
      test     = "StringLike"
      variable = "token.actions.githubusercontent.com:sub"
      values = [
        "repo:${var.github_repo}:ref:refs/heads/main",
        "repo:${local.github_repo_immutable}:ref:refs/heads/main",
      ]
    }
  }
}

# ---------------------------------------------------------------------------
# Permission policies
# ---------------------------------------------------------------------------

data "aws_iam_policy_document" "plan_permissions" {
  statement {
    sid    = "ReadOnlyManagedServices"
    effect = "Allow"
    actions = [
      "s3:Get*",
      "s3:List*",
      "iam:Get*",
      "iam:List*",
      "glue:Get*",
      "glue:List*",
      "glue:BatchGet*",
      "sns:Get*",
      "sns:List*",
      "events:Describe*",
      "events:List*",
      "cloudwatch:Describe*",
      "cloudwatch:List*",
      "logs:Describe*",
      "logs:ListTags*",
    ]
    resources = ["*"]
  }

  # use_lockfile makes the state lock an S3 conditional write, so plan needs
  # object write/delete on the state prefix to acquire and release it.
  statement {
    sid       = "TerraformStateObjects"
    effect    = "Allow"
    actions   = ["s3:GetObject", "s3:PutObject", "s3:DeleteObject"]
    resources = [local.state_objects_arn]
  }

  statement {
    sid       = "TerraformStateBucketList"
    effect    = "Allow"
    actions   = ["s3:ListBucket"]
    resources = [local.state_bucket_arn]
  }

  # The shop extract: VPC, subnet and security-group lookups, and refreshing
  # the extract Lambda and its schedule. Read-only.
  statement {
    sid    = "ReadShopExtract"
    effect = "Allow"
    actions = [
      "ec2:Describe*",
      "lambda:Get*",
      "lambda:List*",
      "scheduler:Get*",
      "scheduler:List*",
    ]
    resources = ["*"]
  }
}

data "aws_iam_policy_document" "deploy_permissions" {
  statement {
    sid       = "TerraformStateObjects"
    effect    = "Allow"
    actions   = ["s3:GetObject", "s3:PutObject", "s3:DeleteObject"]
    resources = [local.state_objects_arn]
  }

  statement {
    sid       = "TerraformStateBucketList"
    effect    = "Allow"
    actions   = ["s3:ListBucket"]
    resources = [local.state_bucket_arn]
  }

  statement {
    sid    = "ManageProjectBuckets"
    effect = "Allow"
    actions = [
      "s3:CreateBucket",
      "s3:DeleteBucket",
      "s3:Get*",
      "s3:List*",
      "s3:PutBucketTagging",
      "s3:PutBucketVersioning",
      "s3:PutEncryptionConfiguration",
      "s3:PutBucketPublicAccessBlock",
      "s3:PutLifecycleConfiguration",
      "s3:PutBucketPolicy",
      "s3:DeleteBucketPolicy",
      "s3:PutBucketNotification",
      "s3:PutObject",
      "s3:PutObjectTagging",
      "s3:GetObject",
      "s3:DeleteObject",
    ]
    resources = [
      local.project_buckets,
      "${local.project_buckets}/*",
    ]
  }

  statement {
    sid    = "ManageGlueExecutionRole"
    effect = "Allow"
    actions = [
      "iam:CreateRole",
      "iam:DeleteRole",
      "iam:GetRole",
      "iam:PassRole",
      "iam:TagRole",
      "iam:UntagRole",
      "iam:TagPolicy",
      "iam:UntagPolicy",
      "iam:ListPolicyTags",
      "iam:CreatePolicy",
      "iam:DeletePolicy",
      "iam:GetPolicy",
      "iam:GetPolicyVersion",
      "iam:CreatePolicyVersion",
      "iam:DeletePolicyVersion",
      "iam:ListPolicyVersions",
      "iam:AttachRolePolicy",
      "iam:DetachRolePolicy",
      "iam:ListAttachedRolePolicies",
      "iam:ListRolePolicies",
      "iam:PutRolePolicy",
      "iam:GetRolePolicy",
      "iam:DeleteRolePolicy",
      "iam:ListInstanceProfilesForRole",
    ]
    resources = [
      local.project_role,
      local.project_policy,
    ]
  }

  statement {
    sid    = "ManageGlueJobs"
    effect = "Allow"
    actions = [
      "glue:CreateJob",
      "glue:UpdateJob",
      "glue:DeleteJob",
      "glue:GetJob",
      "glue:TagResource",
      "glue:UntagResource",
      "glue:GetTags",
    ]
    resources = [local.project_jobs]
  }

  statement {
    sid    = "ManageGlueTriggers"
    effect = "Allow"
    actions = [
      "glue:CreateTrigger",
      "glue:UpdateTrigger",
      "glue:DeleteTrigger",
      "glue:GetTrigger",
      "glue:StartTrigger",
      "glue:StopTrigger",
      "glue:TagResource",
      "glue:UntagResource",
      "glue:GetTags",
    ]
    resources = [local.project_triggers]
  }

  statement {
    sid    = "ManageGlueWorkflows"
    effect = "Allow"
    actions = [
      "glue:CreateWorkflow",
      "glue:UpdateWorkflow",
      "glue:DeleteWorkflow",
      "glue:GetWorkflow",
      "glue:TagResource",
      "glue:UntagResource",
      "glue:GetTags"
    ]
    resources = [local.project_workflows]
  }

  # Each catalog action needs permission on its resource and every resource above it:
  # a database's actions on the database and the catalog, a table's on the table too.
  statement {
    sid    = "ManageGlueDatabases"
    effect = "Allow"
    actions = [
      "glue:CreateDatabase",
      "glue:UpdateDatabase",
      "glue:GetDatabase",
      "glue:TagResource",
      "glue:UntagResource",
      "glue:GetTags",
    ]
    resources = [local.project_catalog, local.project_databases]
  }

  # Deleting a database also needs permission on its tables and user-defined functions.
  statement {
    sid     = "DeleteGlueDatabases"
    effect  = "Allow"
    actions = ["glue:DeleteDatabase"]
    resources = [
      local.project_catalog,
      local.project_databases,
      local.project_tables,
      local.project_functions,
    ]
  }

  statement {
    sid    = "ManageGlueTables"
    effect = "Allow"
    actions = [
      "glue:CreateTable",
      "glue:UpdateTable",
      "glue:DeleteTable",
      "glue:GetTable",
      "glue:GetPartitionIndexes",
    ]
    resources = [local.project_catalog, local.project_databases, local.project_tables]
  }

  # List/read Glue actions ignore the resource element, so a plan refresh
  # during apply needs them account-wide (read-only).
  statement {
    sid       = "GlueReadForRefresh"
    effect    = "Allow"
    actions   = ["glue:GetJobs", "glue:ListJobs"]
    resources = ["*"]
  }

  statement {
    sid    = "ManageShopExtractLambda"
    effect = "Allow"
    actions = [
      "lambda:CreateFunction",
      "lambda:DeleteFunction",
      "lambda:UpdateFunctionCode",
      "lambda:UpdateFunctionConfiguration",
      "lambda:TagResource",
      "lambda:UntagResource",
      "lambda:Get*",
      "lambda:List*",
    ]
    resources = ["arn:aws:lambda:*:${local.account_id}:function:shop-sim-extract"]
  }

  statement {
    sid    = "ManageShopExtractSchedule"
    effect = "Allow"
    actions = [
      "scheduler:CreateSchedule",
      "scheduler:UpdateSchedule",
      "scheduler:DeleteSchedule",
      "scheduler:GetSchedule",
    ]
    resources = ["arn:aws:scheduler:*:${local.account_id}:schedule/default/shop-sim-extract-daily"]
  }

  # The VPC lookups, and Lambda checking the function's subnets and security group.
  statement {
    sid       = "DescribeNetworkForExtract"
    effect    = "Allow"
    actions   = ["ec2:Describe*"]
    resources = ["*"]
  }
  statement {
    sid    = "ManageAlertTopic"
    effect = "Allow"
    actions = [
      "sns:CreateTopic",
      "sns:DeleteTopic",
      "sns:GetTopicAttributes",
      "sns:SetTopicAttributes",
      "sns:TagResource",
      "sns:UntagResource",
      "sns:ListTagsForResource",
      "sns:Subscribe",
      "sns:Unsubscribe",
      "sns:GetSubscriptionAttributes",
      "sns:SetSubscriptionAttributes",
      "sns:ListSubscriptionsByTopic"
    ]
    resources = [local.project_topics] # restricted to the SNS topics related to this data pipeline.
  }

  statement {
    sid    = "ManageAlertRules"
    effect = "Allow"
    actions = [
      "events:PutRule",
      "events:DeleteRule",
      "events:DescribeRule",
      "events:PutTargets",
      "events:RemoveTargets",
      "events:ListTargetsByRule",
      "events:TagResource",
      "events:UntagResource",
      "events:ListTagsForResource"
    ]
    resources = [local.project_rules]
  }

  # The missed-success alarms (modules/alerting).
  statement {
    sid    = "ManageMissedSuccessAlarms"
    effect = "Allow"
    actions = [
      "cloudwatch:PutMetricAlarm",
      "cloudwatch:DeleteAlarms",
      "cloudwatch:DescribeAlarms",
      "cloudwatch:TagResource",
      "cloudwatch:UntagResource",
      "cloudwatch:ListTagsForResource"
    ]
    resources = [local.project_alarms]
  }

  # Their metric filters, on the shared Glue output log group: the filters are named with
  # the project's prefix, but IAM can only scope these actions to the log group.
  statement {
    sid    = "ManageRunRecordFilters"
    effect = "Allow"
    actions = [
      "logs:PutMetricFilter",
      "logs:DeleteMetricFilter",
      "logs:DescribeMetricFilters"
    ]
    resources = [
      local.glue_output_log_group,
      "${local.glue_output_log_group}:*",
      local.extract_log_group,
      "${local.extract_log_group}:*",
      local.landings_log_group,
      "${local.landings_log_group}:*",
    ]
  }

  # The log group for the raw bucket's new-object events (modules/alerting).
  statement {
    sid    = "ManageLandingsLogGroup"
    effect = "Allow"
    actions = [
      "logs:CreateLogGroup",
      "logs:DeleteLogGroup",
      "logs:PutRetentionPolicy",
      "logs:DeleteRetentionPolicy",
      "logs:TagResource",
      "logs:UntagResource",
      "logs:ListTagsForResource",
      "logs:TagLogGroup",
      "logs:UntagLogGroup",
      "logs:ListTagsLogGroup"
    ]
    resources = [local.landings_log_group, "${local.landings_log_group}:*"]
  }

  # Reading a log group back, and the resource policy that lets EventBridge write to it.
  # CloudWatch Logs scopes neither to one log group.
  statement {
    sid    = "DescribeLogGroupsAndPolicies"
    effect = "Allow"
    actions = [
      "logs:DescribeLogGroups",
      "logs:PutResourcePolicy",
      "logs:DeleteResourcePolicy",
      "logs:DescribeResourcePolicies"
    ]
    resources = ["*"]
  }

}

data "aws_iam_policy_document" "scripts_permissions" {
  statement {
    sid       = "WriteScripts"
    effect    = "Allow"
    actions   = ["s3:PutObject"]
    resources = ["arn:aws:s3:::${local.scripts_bucket}/*"]
  }

  statement {
    sid       = "ListScriptsBucket"
    effect    = "Allow"
    actions   = ["s3:ListBucket"]
    resources = ["arn:aws:s3:::${local.scripts_bucket}"]
  }
}

# ---------------------------------------------------------------------------
# CloudTrail trail
# ---------------------------------------------------------------------------

# EventBridge only receives API calls ("AWS API Call via CloudTrail") while a trail is
# logging, so the main stack's stop alert (a rule on glue:BatchStopJobRun) needs one. It
# serves the whole account, so it lives here, applied by an admin, not in the main stack.
# It logs management events that change something (WriteOnly), in this region only.

resource "aws_s3_bucket" "trail" {
  bucket = local.trail_bucket
  tags   = { Component = "cloudtrail" }
}

resource "aws_s3_bucket_public_access_block" "trail" {
  bucket                  = aws_s3_bucket.trail.id
  block_public_acls       = true
  block_public_policy     = true
  ignore_public_acls      = true
  restrict_public_buckets = true
}

resource "aws_s3_bucket_lifecycle_configuration" "trail" {
  bucket = aws_s3_bucket.trail.id

  rule {
    id     = "expire-logs"
    status = "Enabled"
    filter {}
    expiration {
      days = 90
    }
  }
}

data "aws_iam_policy_document" "trail_bucket" {
  statement {
    sid    = "CloudTrailAclCheck"
    effect = "Allow"
    principals {
      type        = "Service"
      identifiers = ["cloudtrail.amazonaws.com"]
    }
    actions   = ["s3:GetBucketAcl"]
    resources = [aws_s3_bucket.trail.arn]
    condition {
      test     = "StringEquals"
      values   = [local.trail_arn]
      variable = "aws:SourceArn"
    }
  }

  statement {
    sid    = "CloudTrailWrite"
    effect = "Allow"
    principals {
      type        = "Service"
      identifiers = ["cloudtrail.amazonaws.com"]
    }
    actions   = ["s3:PutObject"]
    resources = ["${aws_s3_bucket.trail.arn}/AWSLogs/${local.account_id}/*"]
    condition {
      test     = "StringEquals"
      values   = ["bucket-owner-full-control"]
      variable = "s3:x-amz-acl"
    }
    condition {
      test     = "StringEquals"
      values   = [local.trail_arn]
      variable = "aws:SourceArn"
    }
  }
}

resource "aws_s3_bucket_policy" "trail" {
  bucket = aws_s3_bucket.trail.id
  policy = data.aws_iam_policy_document.trail_bucket.json
}

resource "aws_cloudtrail" "management" {
  name                  = local.trail_name
  s3_bucket_name        = aws_s3_bucket.trail.id
  is_multi_region_trail = false

  event_selector {
    read_write_type           = "WriteOnly"
    include_management_events = true
  }

  tags = { Component = "cloudtrail" }

  # CloudTrail checks the bucket policy when the trail is created.
  depends_on = [aws_s3_bucket_policy.trail]
}


# ---------------------------------------------------------------------------
# Roles
# ---------------------------------------------------------------------------

resource "aws_iam_role" "plan" {
  name               = "${local.name_prefix}-cicd-plan"
  assume_role_policy = data.aws_iam_policy_document.plan_trust.json
}

resource "aws_iam_role_policy" "plan" {
  name   = "${local.name_prefix}-cicd-plan"
  role   = aws_iam_role.plan.id
  policy = data.aws_iam_policy_document.plan_permissions.json
}

resource "aws_iam_role" "deploy" {
  name               = "${local.name_prefix}-cicd-deploy"
  assume_role_policy = data.aws_iam_policy_document.deploy_trust.json
}

resource "aws_iam_role_policy" "deploy" {
  name   = "${local.name_prefix}-cicd-deploy"
  role   = aws_iam_role.deploy.id
  policy = data.aws_iam_policy_document.deploy_permissions.json
}

resource "aws_iam_role" "scripts" {
  name               = "${local.name_prefix}-cicd-scripts"
  assume_role_policy = data.aws_iam_policy_document.scripts_trust.json
}

resource "aws_iam_role_policy" "scripts" {
  name   = "${local.name_prefix}-cicd-scripts"
  role   = aws_iam_role.scripts.id
  policy = data.aws_iam_policy_document.scripts_permissions.json
}
