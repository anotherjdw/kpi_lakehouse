# Deployment

## Project Setup

```bash
make install          # .venv from python3.11, pip install -e ".[dev]" into it, then pre-commit install
make test             # full suite
make check            # lint + typecheck + test
```

Glue 5.0 runs Python 3.11, so `make install` builds `.venv` with
`python3.11`, which must be on your `PATH`, and every other target runs the tools in
`.venv/bin`. To run a tool yourself, activate it first: `source .venv/bin/activate`.

`make install` also installs the pre-commit hooks into `.git/hooks/`, so every `git commit` runs
ruff, ruff format and mypy, the checks CI's `quality` job runs. The hooks find `ruff` and `mypy`
on your `PATH`, so commit from a shell where `.venv` is active. In a folder that isn't a git
repository yet, it says so instead: run `git init`, then `.venv/bin/pre-commit install`. When a
hook rewrites a file (ruff format wraps a long line, for example), the commit stops; `git add`
the file and commit again.

Individual test tiers, one folder each: `make test-unit` (`tests/unit`), `make test-integration`
(`tests/integration`), `make test-e2e` (`tests/end_to_end`). Every test runs on a local Spark
session, so Java must be installed.

Copy `.env.example` to `.env` and fill in the values before running anything that touches
AWS.

## Deployment Setup

CI/CD runs from `.github/workflows/ci.yml`. Every push and pull request to the default
branch runs the quality gates, a credential-free `terraform validate`, and a read-only
`terraform plan` (posted as a comment on the PR). A push to the default branch additionally
runs `terraform apply` — gated behind the **`prod` GitHub Environment** — and
then uploads the shared code zip (`lib/kpi_lakehouse.zip`) and each job script to S3. Before the plan, CI also
builds the extract Lambda's zip with `scripts/build_lambda.sh`; `terraform apply` then uploads that zip to the scripts
bucket and updates the Lambda. Only the `plan`, `apply`, and `deploy` jobs assume an AWS role, each a different
least-privilege identity.

**One-time setup (admin), before the first push:** every push runs `ci.yml`, and its
`plan`, `apply` and `deploy` jobs fail until this setup is complete.

Follow every step of "One-time setup" in `infra/terraform/bootstrap/README.md`, in order.
It is the only complete version of these instructions; it covers:

- creating the GitHub repository and connecting this project to it, without pushing;
- making sure the Terraform state bucket exists;
- applying the bootstrap Terraform root as an admin, which creates the GitHub OIDC
  provider and the three CI/CD roles (if your AWS account already has a GitHub OIDC
  provider, import it first, as its "Notes" section describes);
- copying the three role ARNs into the repository's Actions secrets;
- creating the `prod` GitHub Environment with a *Required reviewers* rule;
- adding the `AWS_REGION` repository variable;
- the first push.
