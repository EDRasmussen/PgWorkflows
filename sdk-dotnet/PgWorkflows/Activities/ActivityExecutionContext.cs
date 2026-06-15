namespace PgWorkflows.Activities;

/// <summary>
/// Execution metadata passed to delegate-based activities registered with the
/// <c>RegisterActivity</c> overload that accepts it.
/// </summary>
/// <param name="JobId">The activity job's id in <c>pw_activity_jobs</c>.</param>
/// <param name="ActivityName">The durable activity name the job was enqueued under.</param>
/// <param name="Attempt">The current attempt number, starting at 1.</param>
/// <param name="CreatedAt">When the job was enqueued.</param>
public sealed record ActivityExecutionContext(
    Guid JobId,
    string ActivityName,
    int Attempt,
    DateTimeOffset CreatedAt
);
