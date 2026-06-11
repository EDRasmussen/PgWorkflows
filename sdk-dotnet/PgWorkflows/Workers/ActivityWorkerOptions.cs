namespace PgWorkflows.Workers;

public sealed record ActivityWorkerOptions
{
    public string WorkerId { get; init; } = Environment.MachineName;

    /// <summary>
    /// Jobs leased per database round-trip, capped at <see cref="MaxConcurrency"/>. The worker
    /// dispatches continuously — it refills a freed slot as soon as a job finishes — so this is a
    /// round-trip amortization knob, not a concurrency limit. Leave it and tune
    /// <see cref="MaxConcurrency"/>; lower it only to grab smaller chunks per lease.
    /// </summary>
    public int BatchSize { get; init; } = 16;

    /// <summary>
    /// Maximum number of activities in flight at once. The worker keeps this many running, leasing
    /// a replacement the moment one finishes. The connection pool must be large enough to cover it
    /// (plus the workflow worker's share); startup fails fast otherwise.
    /// </summary>
    public int MaxConcurrency { get; init; } = 10;

    public TimeSpan LeaseDuration { get; init; } = TimeSpan.FromSeconds(30);

    public TimeSpan PollInterval { get; init; } = TimeSpan.FromMilliseconds(250);

    public Func<int, TimeSpan> GetRetryDelay { get; init; } =
        static attempt => TimeSpan.FromSeconds(Math.Min(Math.Max(attempt, 1) * 5, 60));
}
