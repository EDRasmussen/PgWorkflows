namespace PgWorkflows.Workflows;

public sealed record LeasedWorkflowRun(
    Guid WorkflowRunId,
    string WorkflowName,
    string? InputJson,
    WorkflowStatus Status,
    DateTimeOffset CreatedAt,
    DateTimeOffset UpdatedAt,
    DateTimeOffset? CompletedAt,
    string? ResultJson,
    string? Error,
    string LeaseToken,
    DateTimeOffset LeaseExpiresAt,
    string? IdempotencyKey = null,
    int Attempt = 0,
    int MaxAttempts = 1,
    DateTimeOffset VisibleAt = default
);
