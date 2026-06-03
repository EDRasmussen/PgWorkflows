using PgWorkflows.Jobs;

namespace PgWorkflows.Persistence;

public sealed record LeasedActivityJob(
    Guid JobId,
    string ActivityName,
    string? InputJson,
    int Attempt,
    int MaxAttempts,
    DateTimeOffset CreatedAt,
    DateTimeOffset VisibleAt,
    string LeaseToken,
    DateTimeOffset LeaseExpiresAt)
    : ActivityJob(
        JobId,
        ActivityName,
        InputJson,
        JobStatus.Leased,
        Attempt,
        MaxAttempts,
        CreatedAt,
        VisibleAt,
        ResultJson: null,
        Error: null);
