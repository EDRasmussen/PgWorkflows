namespace PgWorkflows.Jobs;

public sealed record EnqueueActivityRequest(
    string ActivityName,
    string? Input,
    int MaxAttempts = 1,
    DateTimeOffset? VisibleAt = null
);
