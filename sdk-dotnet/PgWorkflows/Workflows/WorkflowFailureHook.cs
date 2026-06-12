namespace PgWorkflows.Workflows;

/// <summary>A failure-hook row; InputJson/ResultJson hold the serialized JSON payloads.</summary>
internal sealed record WorkflowFailureHook(
    Guid WorkflowRunId,
    int HookSequence,
    string ActivityName,
    Guid? ActivityJobId,
    string? InputJson,
    WorkflowFailureHookStatus Status,
    DateTimeOffset CreatedAt,
    DateTimeOffset UpdatedAt,
    DateTimeOffset? CompletedAt,
    string? ResultJson,
    string? Error
);
