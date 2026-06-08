namespace PgWorkflows.Workflows;

public sealed record RecordWorkflowFailureHookRequest(
    Guid WorkflowRunId,
    int HookSequence,
    string ActivityName,
    string? InputJson
);
