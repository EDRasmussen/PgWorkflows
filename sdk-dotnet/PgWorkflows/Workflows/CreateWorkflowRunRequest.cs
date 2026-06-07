namespace PgWorkflows.Workflows;

/// <param name="InputJson">Serialized JSON stored as the workflow run input.</param>
public sealed record CreateWorkflowRunRequest(
    string WorkflowName,
    string? InputJson,
    string? IdempotencyKey = null
);
