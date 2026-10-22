<!--
  README.md — created by `engineering-automation scaffold create-project`.

  Sections wrapped in AUTO:BEGIN / AUTO:END markers are owned by the generator and
  will be overwritten when documentation rendering lands. Do not hand-edit inside a
  marker pair, and do not remove the markers. Everything outside them is yours.

  Anything you write here that a transformation needs in order to be *correct* is in
  the wrong file. Dataset semantics belong in the data contract; business logic
  belongs in the job spec. This file is for the operational context that has no other
  home.

  Replace every TODO before this repository is reviewed.
-->

# kpi_lakehouse

> TODO: One sentence. What this data product is, and who depends on it.

## What this pipeline produces

TODO: The datasets this repository owns, and the decision or downstream system each one
serves. Name the consumers. If a dataset has no consumer, say so explicitly — that is
worth knowing.

## Ownership

| | |
| --- | --- |
| Data product | `kpi_lakehouse` |
| Owning team | TODO |
| Primary contact | TODO |
| Escalation channel | TODO |

## Upstream dependencies

TODO: The systems this pipeline reads from and who owns each one. Record refresh cadence,
expected arrival time, and the upstream contact in that source's data contract under
`src/kpi_lakehouse/contracts/` — not here. This section is for the dependencies that
are political rather than technical: who to talk to, and what breaks when they change
something.

## Service expectations

TODO: When must the output be ready, and what happens downstream if it is late? State the
freshness commitment you are actually making, not the one you hope for. If there is no
commitment, write "none" — an honest "none" is more useful than an aspirational SLA.

## Operations

TODO: The runbook. Where do logs go? How do you re-run a single failed window? What is the
backfill procedure, and who has to be told before you start one? What are the known
failure modes and their first diagnostic step?

## Deployment

CI/CD runs from `.github/workflows/ci.yml`. Every push and pull request to the default
branch runs the quality gates, a credential-free `terraform validate`, and a read-only
`terraform plan` (posted as a comment on the PR). A push to the default branch additionally
runs `terraform apply` — gated behind the **`prod` GitHub Environment** — and
then uploads the shared code zip (`lib/kpi_lakehouse.zip`) and each job script to S3.
Only the `plan`, `apply`, and `deploy` jobs assume an AWS role, each a different
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

<!-- AUTO:BEGIN jobs -->
## Jobs

_No ETL jobs are defined yet._

Create each job in this order, from the repository root:

```bash
engineering-automation scaffold create-job-settings . <etl_job>
#   fill in src/kpi_lakehouse/jobs/<etl_job>/<etl_job>.yaml and its output contract,
#   src/kpi_lakehouse/contracts/<etl_job>_data_contract.yaml
engineering-automation scaffold create-source-contracts . <etl_job>
#   fill in each source contract it wrote in src/kpi_lakehouse/contracts/
engineering-automation generate create-job . <etl_job>
#   fill in tests/fixtures/<etl_job>/inputs.py, then set INPUTS_FILLED = True
engineering-automation generate expected-outputs . <etl_job>
#   check every row in tests/fixtures/<etl_job>/expected.py, then set each
#   <TRANSFORMATION>_REVIEWED flag to True
engineering-automation generate codegen . <etl_job>
make check
```

`expected-outputs` and `codegen` call the Claude API; `expected-outputs` never sees the
code. A job that reads another job's output names it in a `job:` source: create that job
first.
<!-- AUTO:END jobs -->

<!-- AUTO:BEGIN lineage -->
## Lineage

_Rendered from the job specs once at least one job exists._
<!-- AUTO:END lineage -->

<!-- AUTO:BEGIN contracts -->
## Data contracts

_Rendered from `src/kpi_lakehouse/contracts/` once at least one contract exists._
<!-- AUTO:END contracts -->

<!-- AUTO:BEGIN quickstart -->
## Getting started

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
<!-- AUTO:END quickstart -->

<!-- AUTO:BEGIN layout -->
## Repository layout

```
src/kpi_lakehouse/
├── contracts/          # data contracts — one YAML per dataset; generation-time input, never shipped
├── jobs/               # one subdirectory per ETL job: job spec, Glue entry point, transformations
├── runtime/            # the run record every job emits
└── utils/              # shared runtime: read, validate and write DataFrames, date windows,
    │                   # register a table's partitions
    └── schema/         # one StructType per data contract in use (tool-owned)
infra/terraform/        # S3, Glue, IAM and the Glue Data Catalog's tables for this data product
└── bootstrap/          # CI/CD OIDC provider + plan/deploy/scripts roles (applied once)
tests/
├── conftest.py         # local Spark session and temporary folders for every test
├── helpers.py          # shared helpers: assertions, fixture rows and fixture files
├── fixtures/           # input and expected rows, one folder per job
├── unit/               # one test per transformation; utils/ tests the shared runtime
├── integration/        # read, transform and write against local files
└── end_to_end/         # a whole job run
```
<!-- AUTO:END layout -->

<!-- AUTO:BEGIN changing -->
## How to change things

| To change | Edit | Then run |
| --- | --- | --- |
| A dataset's columns or types | its data contract in `contracts/` | `generate codegen` |
| What a transformation does | its `description` in the job spec; replace its function's body with `raise NotImplementedError` | `generate codegen` |
| A transformation's expected rows | set its list in `tests/fixtures/<etl_job>/expected.py` back to `[]` and its REVIEWED flag to `False` | `generate expected-outputs` |
| A job's Glue workers, processing or schedule | the job spec | `generate codegen` |
| A table's description or column comments | `description` in its output contract and on its columns | `generate codegen` |
| Whether a job's output has a table | `table: hive` in its output contract; for a job `create-job` has already rendered, also its code (`parse_arguments`, `write` and `<etl_job>.py`) and its tests, as engineering-automation's README lists | `generate codegen` |
| Infrastructure | `infra/terraform/`, outside the `AMEND:JOBS` and `AMEND:TABLES` markers | `make plan` |

`generate codegen` rebuilds the StructType module (`utils/schema/schema_definitions.py`) and
the Terraform jobs, trigger and tables maps from the job specs and contracts, whether or not it fills
a function. `create-job` never overwrites a file, and `expected-outputs` and `codegen` fill
only lists and function bodies that are still empty, so every edit you make to generated code
and rows is kept. It also means a change to a job's sources, sink, transformations or
contracts doesn't reach the code, tests and rows already rendered for that job: edit those
by hand.
<!-- AUTO:END changing -->
