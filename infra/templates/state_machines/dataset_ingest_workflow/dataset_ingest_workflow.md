# Dataset Ingest Workflow

Parent state machine that orchestrates three child state machines (download, subset, transform).
Each child SM is an independent redrivable unit. Skip logic detects already-completed task runs
via DynamoDB and bypasses the corresponding child SM invocation.

```mermaid
flowchart TD
    Start([Start]) --> MarkReleaseStarted

    MarkReleaseStarted["MarkReleaseStarted\n(Lambda)\nRelease status → STARTED"]
    MarkReleaseStarted --> GetDownloadTaskRunStatus

    GetDownloadTaskRunStatus["GetDownloadTaskRunStatus\n(Lambda)\n$.task_run_check ← status, run_id"]
    GetDownloadTaskRunStatus --> SkipDownloadCheck

    SkipDownloadCheck{SkipDownloadCheck}
    SkipDownloadCheck -->|task_run_status = COMPLETED| CheckHasTransform
    SkipDownloadCheck -->|default| InvokeDownloadChildSM

    InvokeDownloadChildSM["InvokeDownloadChildSM\n(states:startExecution.sync:2)\ndmpworks-{env}-download"]
    InvokeDownloadChildSM -->|success| CheckHasTransform
    InvokeDownloadChildSM -->|catch States.ALL| WorkflowFailed

    CheckHasTransform{CheckHasTransform}
    CheckHasTransform -->|dataset = ror\nor data-citation-corpus| MarkReleaseCompleted
    CheckHasTransform -->|default| CheckUseSubset

    CheckUseSubset{CheckUseSubset}
    CheckUseSubset -->|use_subset = true| GetSubsetTaskRunStatus
    CheckUseSubset -->|default| GetTransformTaskRunStatus

    GetSubsetTaskRunStatus["GetSubsetTaskRunStatus\n(Lambda)\n$.task_run_check ← status, run_id"]
    GetSubsetTaskRunStatus --> SkipSubsetCheck

    SkipSubsetCheck{SkipSubsetCheck}
    SkipSubsetCheck -->|task_run_status = COMPLETED| GetTransformTaskRunStatus
    SkipSubsetCheck -->|default| InvokeSubsetChildSM

    InvokeSubsetChildSM["InvokeSubsetChildSM\n(states:startExecution.sync:2)\ndmpworks-{env}-subset"]
    InvokeSubsetChildSM -->|success| GetTransformTaskRunStatus
    InvokeSubsetChildSM -->|catch States.ALL| WorkflowFailed

    GetTransformTaskRunStatus["GetTransformTaskRunStatus\n(Lambda)\n$.task_run_check ← status, run_id"]
    GetTransformTaskRunStatus --> SkipTransformCheck

    SkipTransformCheck{SkipTransformCheck}
    SkipTransformCheck -->|task_run_status = COMPLETED| MarkReleaseCompleted
    SkipTransformCheck -->|default| InvokeTransformChildSM

    InvokeTransformChildSM["InvokeTransformChildSM\n(states:startExecution.sync:2)\ndmpworks-{env}-transform"]
    InvokeTransformChildSM -->|success| MarkReleaseCompleted
    InvokeTransformChildSM -->|catch States.ALL| WorkflowFailed

    MarkReleaseCompleted["MarkReleaseCompleted\n(Lambda)\nRelease status → COMPLETED"]
    MarkReleaseCompleted --> End([End])

    SetWorkflowFailedStatus["SetWorkflowFailedStatus\n(Pass)\n$.workflow_run_status ← FAILED"]
    SetWorkflowFailedStatus --> HandleWorkflowFailure

    HandleWorkflowFailure["HandleWorkflowFailure\n(Lambda)\nRelease status → FAILED"]
    HandleWorkflowFailure --> WorkflowFailed

    WorkflowFailed([Fail])
```

## Child state machine pattern

Each child SM (download / subset / transform) follows the same pattern:

```mermaid
flowchart TD
    Start([Start]) --> BuildParams

    BuildParams["Build{X}Params\n(Lambda)\nGenerate run_id, build Batch params\n$.current ← run_id, run_name, batch_params"]
    BuildParams -->|success| Submit
    BuildParams -->|catch States.ALL| SetTaskRunFailedStatus

    Submit["Submit{X}\n(Batch .sync)"]
    Submit -->|success| SetTaskRunComplete
    Submit -->|catch States.ALL| SetTaskRunFailedStatus

    SetTaskRunComplete["Set{X}TaskRunComplete\n(Lambda)\nRun → COMPLETED\nRelease.{x}_run_id ← current.run_id"]
    SetTaskRunComplete --> End([End])

    SetTaskRunFailedStatus["SetTaskRunFailedStatus\n(Pass)\n$.workflow_run_status ← FAILED"]
    SetTaskRunFailedStatus --> HandleTaskRunFailed

    HandleTaskRunFailed["HandleTaskRunFailed\n(Lambda)\nRun → FAILED\nRelease status → FAILED"]
    HandleTaskRunFailed --> TaskRunFailed([Fail])
```

## Error handling

- **Child SM failures**: The child SM's own handler marks both the `TaskRunRecord` and the release status as FAILED before reaching its `TaskRunFailed` state. The parent catches the child SM failure and jumps directly to `WorkflowFailed` — no additional Lambda call needed.
- **Parent-level failures** (MarkReleaseStarted, GetTaskRunStatus): Route through `SetWorkflowFailedStatus` → `HandleWorkflowFailure` → `WorkflowFailed`.

## Skip logic

On fresh re-runs, `GetXxxTaskRunStatus` checks whether `{task_type}_run_id` is set on `DatasetReleaseRecord`. That field is only written after a task run completes successfully, so its presence guarantees the task run is safe to skip. If `task_run_status == "COMPLETED"`, the corresponding child SM invocation is bypassed entirely.
