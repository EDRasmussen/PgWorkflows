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

    /// <summary>
    /// Upper bound on how long a finished handler's outcome may take to persist while the
    /// worker is shutting down, so a slow or unreachable store can't hang graceful stop.
    /// </summary>
    public TimeSpan ShutdownWriteTimeout { get; init; } = TimeSpan.FromSeconds(5);

    /// <summary>
    /// How often a running activity's lease is renewed. When unset, defaults to a third
    /// of <see cref="LeaseDuration"/>, leaving room for a couple of renewals before the
    /// lease would lapse. If renewals keep failing past the actual expiry, the running
    /// handler is cancelled so another worker can reclaim the job.
    /// </summary>
    public TimeSpan? LeaseRenewalInterval { get; init; }

    public TimeSpan PollInterval { get; init; } = TimeSpan.FromMilliseconds(250);

    public Func<int, TimeSpan> GetRetryDelay { get; init; } =
        static attempt => TimeSpan.FromSeconds(Math.Min(Math.Max(attempt, 1) * 5, 60));

    // A third of the lease leaves room for a couple of renewals before expiry. A
    // configured interval is honoured only when it's a positive value shorter than the
    // lease itself — a longer one would let the lease lapse before the first renewal.
    internal TimeSpan EffectiveRenewalInterval =>
        LeaseRenewalInterval is { } interval
        && interval > TimeSpan.Zero
        && interval < LeaseDuration
            ? interval
            : TimeSpan.FromTicks(LeaseDuration.Ticks / 3);
}
