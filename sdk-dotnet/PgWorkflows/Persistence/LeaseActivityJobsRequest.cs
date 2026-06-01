namespace PgWorkflows.Persistence;

public sealed record LeaseActivityJobsRequest(
    string WorkerId,
    int BatchSize,
    TimeSpan LeaseDuration,
    DateTimeOffset Now);
