namespace PgWorkflows.Jobs;

/// <summary>An activity job row; InputJson/ResultJson hold the serialized JSON payloads.</summary>
internal record ActivityJob(
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
