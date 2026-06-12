namespace PgWorkflows.Workflows;

internal sealed record LeasedWorkflowRun(
    Guid WorkflowRunId,
    string WorkflowName,
    int Attempt,
    int MaxAttempts,
    string LeaseToken,
    DateTimeOffset LeaseExpiresAt
);
