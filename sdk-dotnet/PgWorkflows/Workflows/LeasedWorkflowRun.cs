namespace PgWorkflows.Workflows;

public sealed record LeasedWorkflowRun(
    Guid WorkflowRunId,
    string WorkflowName,
    string? InputJson,
    WorkflowStatus Status,
    DateTimeOffset CreatedAt,
    string LeaseToken,
    DateTimeOffset LeaseExpiresAt,
    string? IdempotencyKey = null,
    int Attempt = 0,
    int MaxAttempts = 1
);
