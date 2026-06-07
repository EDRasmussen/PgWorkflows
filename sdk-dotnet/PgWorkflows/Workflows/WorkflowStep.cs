namespace PgWorkflows.Workflows;

/// <param name="InputJson">Serialized JSON supplied to the activity step.</param>
/// <param name="ResultJson">Serialized JSON recorded when the activity step completed.</param>
public sealed record WorkflowStep(
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
