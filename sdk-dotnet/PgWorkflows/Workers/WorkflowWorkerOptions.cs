namespace PgWorkflows.Workers;

public sealed record WorkflowWorkerOptions
{
    public string WorkerId { get; init; } = Environment.MachineName;

    public int BatchSize { get; init; } = 16;

    public int MaxConcurrency { get; init; } = Environment.ProcessorCount;

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
