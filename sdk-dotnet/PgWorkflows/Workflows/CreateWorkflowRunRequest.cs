namespace PgWorkflows.Workflows;

/// <summary>InputJson is the serialized JSON stored as the workflow run input.</summary>
internal sealed record CreateWorkflowRunRequest(
    string WorkflowName,
    string? InputJson,
    string? IdempotencyKey = null
);
