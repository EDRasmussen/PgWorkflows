namespace PgWorkflows.Workflows;

public sealed record LeaseWorkflowRunsRequest(
    string WorkerId,
    int Limit,
    TimeSpan LeaseDuration,
    DateTimeOffset Now
);
