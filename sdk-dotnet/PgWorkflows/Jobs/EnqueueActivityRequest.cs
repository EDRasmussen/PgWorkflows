namespace PgWorkflows.Jobs;

/// <param name="InputJson">Serialized JSON stored in the activity job's <c>jsonb</c> payload column.</param>
public sealed record EnqueueActivityRequest(
    string ActivityName,
    string? InputJson,
    int MaxAttempts = 1,
    DateTimeOffset? VisibleAt = null
);
