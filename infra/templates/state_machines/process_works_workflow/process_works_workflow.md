# Process Works Workflow

Orchestrates the monthly process-works pipeline: waits for all five dataset ingests
to complete, runs SQLMesh to build the unified works index, syncs the result to
OpenSearch, and finally fires the process-dmps workflow.

## Trigger

EventBridge scheduled rule: `cron(0 16 8-14 * MON *)` — second Monday of each month
at 16:00 UTC. The `StartProcessWorksFunction` Lambda computes `second_monday_of_month(today)`
as the `publication_date` / `run_date` and starts the `ProcessWorksStateMachine`.

## Orchestration Diagram

```
StartProcessWorksFunction (EventBridge schedule: second Monday 16:00 UTC)
        │
        ▼
ProcessWorksStateMachine
  ├── CheckDatasetsReady (Lambda)
  │     Checks all 5 required TaskCheckpoints exist + no release in STARTED state
  │     Returns: {all_ready, run_id_openalex_works, run_id_datacite,
  │              run_id_crossref_metadata, run_id_ror, run_id_data_citation_corpus,
  │              run_id_sqlmesh_prev}
  │
  ├── AreAllDatasetsReady (Choice)
  │     all_ready == false → WaitForDatasets (Wait 3600s) → CheckDatasetsReady
  │     all_ready == true  → MergeRunIds
  │
  ├── MergeRunIds (Pass) — flatten check.datasets.* into top-level state
  │
  ├── CreateProcessWorksRun (Lambda)
  │     Generates run_id, creates ProcessWorksRunRecord (STARTED)
  │
  ├── MergeRunId (Pass) — merge created.run_id into top-level state
  │
  ├── MarkProcessWorksStarted (Lambda: SetProcessWorksRunStatusFunction)
  │     Sets status=STARTED, records execution_arn
  │
  ├── GetSqlmeshTaskRunStatus (Lambda: GetTaskRunStatusFunction)
  │     workflow_key="process-works", task_type="sqlmesh"
  │
  ├── SkipSqlmeshCheck (Choice)
  │     COMPLETED → GetSyncWorksTaskRunStatus
  │     else      → InvokeSqlmeshChildSM
  │
  ├── InvokeSqlmeshChildSM (waitForTaskToken → SqlmeshWorkflowStateMachine)
  │     ├── BuildSqlmeshParams (GetBatchJobParamsFunction)
  │     ├── SubmitSqlmesh (batch:submitJob.sync)
  │     ├── SetSqlmeshTaskRunComplete (SetTaskRunCompleteFunction)
  │     └── SendTaskSuccess → parent
  │
  ├── GetSyncWorksTaskRunStatus (Lambda: GetTaskRunStatusFunction)
  │     workflow_key="process-works", task_type="sync-works"
  │     ResultPath: $.task_run_check  (task_run_id = sqlmesh run_id)
  │
  ├── SkipSyncWorksCheck (Choice)
  │     COMPLETED → MarkProcessWorksCompleted
  │     else      → InvokeSyncWorksChildSM
  │
  ├── InvokeSyncWorksChildSM (waitForTaskToken → SyncWorksWorkflowStateMachine)
  │     Input includes sqlmesh_run_id from $.task_run_check.task_run_id
  │     ├── BuildSyncWorksParams (GetBatchJobParamsFunction)
  │     ├── SubmitSyncWorks (batch:submitJob.sync)
  │     ├── SetSyncWorksTaskRunComplete (SetTaskRunCompleteFunction)
  │     └── SendTaskSuccess → parent
  │
  ├── MarkProcessWorksCompleted (Lambda: SetProcessWorksRunStatusFunction)
  │
  └── StartProcessDmps (states:startExecution, fire-and-forget → ProcessDmpsStateMachine)
```

## Required Datasets

| Pool Key                  | Workflow Key           | Task Name   |
|---------------------------|------------------------|-------------|
| run_id_openalex_works     | openalex-works         | transform   |
| run_id_datacite           | datacite               | transform   |
| run_id_crossref_metadata  | crossref-metadata      | transform   |
| run_id_ror                | ror                    | download    |
| run_id_data_citation_corpus | data-citation-corpus | download    |

All five must have a completed `TaskCheckpointRecord` and no `DatasetReleaseRecord`
in `STARTED` state before the pipeline proceeds.

## DynamoDB Records

- **ProcessWorksRunRecord** (`dmpworks-{env}-process-works-runs`): Keyed by
  `(run_date, run_id)`. Tracks the overall run status (STARTED → COMPLETED | FAILED)
  and the run IDs of all input datasets plus the SQLMesh output run_id.

- **TaskCheckpointRecord** (`dmpworks-{env}-task-checkpoints`): Written by child SMs
  via `SetTaskRunCompleteFunction`. Used by `GetTaskRunStatusFunction` to enable
  skip-on-retry semantics for sqlmesh and sync-works steps.

## Failure Handling

EventBridge routes `FAILED` executions for `ProcessWorksStateMachine`,
`SqlmeshWorkflowStateMachine`, and `SyncWorksWorkflowStateMachine` to
`HandleExecutionFailureFunction`. For `workflow_key == "process-works"`,
the handler calls `set_process_works_run_status(status="FAILED")` instead
of `update_release_status`.

## Incremental SQLMesh Runs

The `run_id_sqlmesh_prev` field in `ProcessWorksRunRecord` is populated from
the most recent `TaskCheckpointRecord` for `(workflow_key="process-works", task_name="sqlmesh")`,
or `SQLMESH_INITIAL_RUN_ID = "INITIAL"` if no prior run exists. This enables SQLMesh
to perform incremental updates against the previous snapshot.
