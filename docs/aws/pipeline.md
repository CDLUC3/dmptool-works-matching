# Pipeline Management

`dmpworks pipeline` enable management of Step Functions workflows, DynamoDB state, 
and EventBridge schedules.

## Prerequisites

Authenticate with AWS before running any pipeline commands (see
[AWS Credentials](batch.md#11-aws-credentials) for setup).

```bash
export AWS_PROFILE=my-profile
aws sso login
```

## Common Options

All commands require an environment. Set it once per session:

```bash
export AWS_ENV=dev
```

Or pass it per command with `--env`:

```bash
dmpworks pipeline show status --env dev
```

Examples below assume `AWS_ENV` is set.

## Show

Read-only inspection commands that query DynamoDB and AWS.

### show status

Display the full pipeline dashboard: latest dataset releases, task checkpoints,
and recent process-works and process-dmps runs.

```bash
dmpworks pipeline show status
```

### show releases

Display dataset release records. Defaults to releases from the last 3 months.

```bash
dmpworks pipeline show releases
dmpworks pipeline show releases --start-date 2025-01-01
```

### show checkpoints

Display task checkpoint records. Defaults to the last 3 months.

```bash
dmpworks pipeline show checkpoints
dmpworks pipeline show checkpoints --start-date 2025-01-01 --end-date 2025-06-01
```

### show processes

Display process-works and process-dmps run records. Defaults to the last 3 months.

```bash
dmpworks pipeline show processes
dmpworks pipeline show processes --start-date 2025-01-01 --end-date 2025-06-01
```

### show new-versions

Invoke the version checker Lambda in dry-run mode to discover new upstream
dataset versions without triggering any ingests.

```bash
dmpworks pipeline show new-versions
```

## Runs

Workflow execution history and actions.

### runs list

Show Step Functions execution history with nested child executions. Supports
date range and status filtering.

```bash
dmpworks pipeline runs list
dmpworks pipeline runs list --start-date 2025-06-01 --status FAILED
```

### runs start

Interactive wizard to start a Step Functions execution. The wizard prompts for:

1. **Workflow type** — ingest, process-works, or process-dmps.
2. **Dataset and release date** (ingest) or **release date** (process-works/dmps).
3. **Checkpoint review** — shows which steps have completed checkpoints.
4. **Step selection** — choose which steps to re-run vs skip. Steps selected
   for re-run have their checkpoints deleted before the execution starts.
5. **Pre-flight checks** — warns about running executions and enabled schedules.
6. **Confirmation** — displays the full execution input and asks for confirmation.

For process-works, the wizard also prompts whether to chain into process-dmps
after completion and which DMP scope to use (all DMPs or recently modified only).

```bash
dmpworks pipeline runs start
```

### runs approve-retry

Interactive wizard for approving failed child workflows for retry. The parent
state machine pauses at an approval gate when a child fails, waiting up to
7 days for manual approval.

The wizard:

1. Scans DynamoDB for runs with pending approval tokens.
2. Validates that parent executions are still running (auto-clears stale tokens).
3. Shows a tree view of retryable executions.
4. Prompts to select and approve a retry.

```bash
dmpworks pipeline runs approve-retry
```

## Schedules

Show, pause, or resume EventBridge schedule rules.

### schedules list

List all EventBridge schedule rules with cron expressions and next run times.

```bash
dmpworks pipeline schedules list
```

### schedules pause

Disable EventBridge schedule rules. Use `--rule` to target a specific rule,
or omit to pause all rules.

```bash
dmpworks pipeline schedules pause
dmpworks pipeline schedules pause --rule version-checker-schedule
```

### schedules resume

Re-enable EventBridge schedule rules. Use `--rule` to target a specific rule,
or omit to resume all rules.

```bash
dmpworks pipeline schedules resume
dmpworks pipeline schedules resume --rule process-dmps-schedule
```

## Admin

Operational and maintenance commands.

### admin cleanup-s3

Show stale S3 prefixes from old pipeline runs, then optionally schedule them
for deletion via S3 lifecycle rules.

```bash
dmpworks pipeline admin cleanup-s3
```

### admin delete-checkpoints

Interactively select and delete task checkpoints from DynamoDB. Displays all
checkpoints, then prompts with a multi-select list.

```bash
dmpworks pipeline admin delete-checkpoints
```

## Command Reference

| Command | Description |
| --- | --- |
| `show status` | Full pipeline dashboard |
| `show releases` | Dataset release records |
| `show checkpoints` | Task checkpoint records |
| `show processes` | Process-works and process-dmps runs |
| `show new-versions` | Check for new upstream dataset versions |
| `runs list` | Step Functions execution history |
| `runs start` | Interactive wizard to start a workflow |
| `runs approve-retry` | Approve a failed child workflow for retry |
| `schedules list` | List EventBridge schedule rules |
| `schedules pause` | Disable schedule rules |
| `schedules resume` | Enable schedule rules |
| `admin cleanup-s3` | Show and apply S3 cleanup plan |
| `admin delete-checkpoints` | Interactively delete task checkpoints |
