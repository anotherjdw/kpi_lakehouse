# main.tf -- CI/CD identities for kpi_lakehouse, provisioned as code.
# Creates the GitHub OIDC provider and three least-privilege roles the data
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
  account_id       = data.aws_caller_identity.current.account_id
  project_role     = "arn:aws:iam::${local.account_id}:role/${local.name_prefix}-*"
  project_policy   = "arn:aws:iam::${local.account_id}:policy/${local.name_prefix}-*"
  project_buckets  = "arn:aws:s3:::${local.bucket_prefix}-*"
  project_jobs     = "arn:aws:glue:*:${local.account_id}:job/${local.name_prefix}-*"
  project_triggers = "arn:aws:glue:*:${local.account_id}:trigger/${local.name_prefix}-*"

  # Catalog names hold no hyphens: a layer's database is named after its bucket with
  # "-" turned into "_" (modules/catalog in the main stack).
  catalog_prefix    = replace(local.bucket_prefix, "-", "_")
  project_catalog   = "arn:aws:glue:*:${local.account_id}:catalog"
  project_databases = "arn:aws:glue:*:${local.account_id}:database/${local.catalog_prefix}_*"
  project_tables    = "arn:aws:glue:*:${local.account_id}:table/${local.catalog_prefix}_*/*"
  project_functions = "arn:aws:glue:*:${local.account_id}:userDefinedFunction/${local.catalog_prefix}_*/*"
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
      "s3:PutObject",
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
