namespace PgWorkflows.Workers;

/// <summary>
/// Tunes the hosted workflow worker. Configure via
/// <see cref="PgWorkflows.PgWorkflowsBuilder.ConfigureWorkflowWorker"/>.
/// </summary>
public sealed record WorkflowWorkerOptions
{
    /// <summary>Identifies this worker in leases and logs. Defaults to the machine name.</summary>
    public string WorkerId { get; init; } = Environment.MachineName;

    /// <summary>
    /// Runs leased per database round-trip, capped at <see cref="MaxConcurrency"/>. The worker
    /// dispatches continuously (it refills a freed slot as soon as a run finishes), so this is a
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

    /// <summary>
    /// How long a lease lives between heartbeats. The shared heartbeat renews all of a worker's
    /// leases at a third of this interval; a worker that dies stops renewing and its runs are
    /// reclaimed once the lease expires.
    /// </summary>
    public TimeSpan LeaseDuration { get; init; } = TimeSpan.FromSeconds(30);

    /// <summary>How often an idle worker polls for runnable workflow runs.</summary>
    public TimeSpan PollInterval { get; init; } = TimeSpan.FromMilliseconds(250);

    /// <summary>
    /// Safety-net deadline a run is parked for while waiting on activity steps (<c>ctx.Activity</c> /
    /// <c>ctx.WhenAll</c>). The run is normally woken earlier by the edge-trigger when its last
    /// outstanding activity completes; this only bounds how long a parked run lingers if that wake is
    /// ever missed. Keep it well above <see cref="PollInterval"/> so idle parked runs are not
    /// effectively polled.
    /// </summary>
    public TimeSpan ParkGrace { get; init; } = TimeSpan.FromSeconds(30);

    /// <summary>
    /// Whole-workflow attempts before a failure is terminal. Distinct from activity retries;
    /// parks (sleep, signal, activity waits) and transient database errors do not consume an
    /// attempt.
    /// </summary>
    public int MaxAttempts { get; init; } = 1;

    /// <summary>
    /// Backoff between workflow attempts, given the attempt number that just failed. Also used as
    /// the delay before a run released by a transient database error becomes visible again.
    /// </summary>
    public Func<int, TimeSpan> GetRetryDelay { get; init; } =
        static attempt => TimeSpan.FromSeconds(Math.Min(Math.Max(attempt, 1) * 5, 60));
}
