namespace PgWorkflows.Workflows;

/// <summary>A step-ledger row; InputJson/ResultJson hold the serialized JSON payloads.</summary>
internal sealed record WorkflowStep(
    Guid WorkflowRunId,
    int StepSequence,
    string ActivityName,
    Guid ActivityJobId,
    string? InputJson,
    WorkflowStepStatus Status,
    DateTimeOffset CreatedAt,
    DateTimeOffset UpdatedAt,
    DateTimeOffset? CompletedAt,
    string? ResultJson,
    string? Error
);
