namespace PgWorkflows.Workflows;

/// <summary>A workflow run row; InputJson/ResultJson hold the serialized JSON payloads.</summary>
internal sealed record WorkflowRun(
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
