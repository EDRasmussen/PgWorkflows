namespace PgWorkflows.Workflows;

/// <param name="InputJson">Serialized JSON supplied to the failure hook activity.</param>
/// <param name="ResultJson">Serialized JSON recorded when the failure hook completed.</param>
public sealed record WorkflowFailureHook(
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
