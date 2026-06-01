namespace PgWorkflows.Jobs;

public record ActivityJob(
    Guid JobId,
    string ActivityName,
    string? Input,
    JobStatus Status,
    int Attempt,
    int MaxAttempts,
    DateTimeOffset CreatedAt,
    DateTimeOffset VisibleAt,
    string? Result,
    string? Error
);
