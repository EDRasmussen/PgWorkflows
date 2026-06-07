namespace PgWorkflows.Workflows;

/// <param name="InputJson">Serialized JSON supplied to the activity job.</param>
public sealed record RecordWorkflowStepRequest(
    Guid WorkflowRunId,
    int StepSequence,
    string ActivityName,
    Guid ActivityJobId,
    string? InputJson
);
