namespace PgWorkflows.Workers;

public sealed record WorkflowWorkerOptions
{
    public string WorkerId { get; init; } = Environment.MachineName;

    public int BatchSize { get; init; } = 16;

    public int MaxConcurrency { get; init; } = Environment.ProcessorCount;

    public TimeSpan LeaseDuration { get; init; } = TimeSpan.FromSeconds(30);

    public TimeSpan? LeaseRenewalInterval { get; init; }

    public TimeSpan PollInterval { get; init; } = TimeSpan.FromMilliseconds(250);

    public int MaxAttempts { get; init; } = 1;

    public Func<int, TimeSpan> GetRetryDelay { get; init; } =
        static attempt => TimeSpan.FromSeconds(Math.Min(Math.Max(attempt, 1) * 5, 60));

    internal TimeSpan EffectiveRenewalInterval =>
        LeaseRenewalInterval is { } interval
        && interval > TimeSpan.Zero
        && interval < LeaseDuration
            ? interval
            : TimeSpan.FromTicks(LeaseDuration.Ticks / 3);
}
