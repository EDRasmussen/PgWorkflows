namespace PgWorkflows.Persistence;

internal sealed record LeasedActivityJob(
    Guid JobId,
    string ActivityName,
    string? InputJson,
    int Attempt,
    int MaxAttempts,
    DateTimeOffset CreatedAt,
    string LeaseToken,
    DateTimeOffset LeaseExpiresAt
);
