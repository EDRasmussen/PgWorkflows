namespace PgWorkflows.Jobs;

/// <param name="InputJson">Serialized JSON read from the activity job's input payload.</param>
/// <param name="ResultJson">Serialized JSON read from the activity job's result payload.</param>
public record ActivityJob(
    Guid JobId,
    string ActivityName,
    string? InputJson,
    JobStatus Status,
    int Attempt,
    int MaxAttempts,
    DateTimeOffset CreatedAt,
    DateTimeOffset VisibleAt,
    string? ResultJson,
    string? Error,
    string? IdempotencyKey = null
);
