namespace PgWorkflows.Workflows;

/// <param name="InputJson">Serialized JSON supplied when the workflow run was created.</param>
/// <param name="ResultJson">Serialized JSON recorded when the workflow run completed.</param>
public sealed record WorkflowRun(
    Guid WorkflowRunId,
    string WorkflowName,
    string? InputJson,
    WorkflowStatus Status,
    DateTimeOffset CreatedAt,
    DateTimeOffset UpdatedAt,
    DateTimeOffset? CompletedAt,
    string? ResultJson,
    string? Error,
    string? IdempotencyKey = null,
    int Attempt = 0,
    int MaxAttempts = 1,
    DateTimeOffset VisibleAt = default
);
