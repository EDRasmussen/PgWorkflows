namespace PgWorkflows.Activities;

public sealed record ActivityExecutionContext(
    Guid JobId,
    string ActivityName,
    int Attempt,
    DateTimeOffset CreatedAt);
