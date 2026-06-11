namespace PgWorkflows.Workers;

public sealed record WorkflowWorkerOptions
{
    public string WorkerId { get; init; } = Environment.MachineName;

    /// <summary>
    /// Runs leased per database round-trip, capped at <see cref="MaxConcurrency"/>. The worker
    /// dispatches continuously — it refills a freed slot as soon as a run finishes — so this is a
    /// round-trip amortization knob, not a concurrency limit. Leave it and tune
    /// <see cref="MaxConcurrency"/>; lower it only to grab smaller chunks per lease.
    /// </summary>
    public int BatchSize { get; init; } = 16;

    /// <summary>
    /// Maximum number of workflow runs in flight at once. The worker keeps this many running,
    /// leasing a replacement the moment one finishes. The connection pool must be large enough to
    /// cover it (plus the activity worker's share); startup fails fast otherwise.
    /// </summary>
    public int MaxConcurrency { get; init; } = 10;

    public TimeSpan LeaseDuration { get; init; } = TimeSpan.FromSeconds(30);

    public TimeSpan PollInterval { get; init; } = TimeSpan.FromMilliseconds(250);

    /// <summary>
    /// Safety-net deadline a run is parked for while waiting on activity steps (<c>ctx.Activity</c> /
    /// <c>ctx.WhenAll</c>). The run is normally woken earlier by the edge-trigger when its last
    /// outstanding activity completes; this only bounds how long a parked run lingers if that wake is
    /// ever missed. Keep it well above <see cref="PollInterval"/> so idle parked runs are not
    /// effectively polled.
    /// </summary>
    public TimeSpan ParkGrace { get; init; } = TimeSpan.FromSeconds(30);

    public int MaxAttempts { get; init; } = 1;

    public Func<int, TimeSpan> GetRetryDelay { get; init; } =
        static attempt => TimeSpan.FromSeconds(Math.Min(Math.Max(attempt, 1) * 5, 60));
}
