namespace PgWorkflows.Workers;

public sealed record ActivityWorkerOptions
{
    public string WorkerId { get; init; } = Environment.MachineName;

    /// <summary>
    /// Maximum number of jobs leased per poll. Only takes effect when set below
    /// <see cref="MaxConcurrency"/>: the worker never leases more than it can run at once,
    /// since a job waiting behind the concurrency limit has no lease renewer. Lower it to
    /// grab smaller chunks per poll; otherwise leave it and tune <see cref="MaxConcurrency"/>.
    /// </summary>
    public int BatchSize { get; init; } = 16;

    /// <summary>
    /// Maximum number of activities executed concurrently, and the cap on how many jobs
    /// are leased per poll. Defaults to four per processor, suited to the IO-bound
    /// activities typical of a job queue; lower it for CPU-bound work.
    /// </summary>
    public int MaxConcurrency { get; init; } = Environment.ProcessorCount * 4;

    public TimeSpan LeaseDuration { get; init; } = TimeSpan.FromSeconds(30);

    public TimeSpan PollInterval { get; init; } = TimeSpan.FromMilliseconds(250);

    public Func<int, TimeSpan> GetRetryDelay { get; init; } =
        static attempt => TimeSpan.FromSeconds(Math.Min(Math.Max(attempt, 1) * 5, 60));
}
