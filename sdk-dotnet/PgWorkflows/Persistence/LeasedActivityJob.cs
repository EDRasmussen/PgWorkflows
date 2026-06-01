using PgWorkflows.Jobs;

namespace PgWorkflows.Persistence;

public sealed record LeasedActivityJob(
    Guid JobId,
    string ActivityName,
    string? Input,
    int Attempt,
    int MaxAttempts,
    DateTimeOffset CreatedAt,
    DateTimeOffset VisibleAt,
    string LeaseToken,
    DateTimeOffset LeaseExpiresAt)
    : ActivityJob(
        JobId,
        ActivityName,
        Input,
        JobStatus.Leased,
        Attempt,
        MaxAttempts,
        CreatedAt,
        VisibleAt,
        Result: null,
        Error: null);
