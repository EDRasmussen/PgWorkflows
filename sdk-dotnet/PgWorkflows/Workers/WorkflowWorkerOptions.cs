namespace PgWorkflows.Workers;

public sealed record WorkflowWorkerOptions
{
    public string WorkerId { get; init; } = Environment.MachineName;

    public int BatchSize { get; init; } = 16;

    public int MaxConcurrency { get; init; } = Environment.ProcessorCount;

    public TimeSpan LeaseDuration { get; init; } = TimeSpan.FromSeconds(30);

    public TimeSpan? LeaseRenewalInterval { get; init; }

    public TimeSpan PollInterval { get; init; } = TimeSpan.FromMilliseconds(250);

    internal TimeSpan EffectiveRenewalInterval =>
        LeaseRenewalInterval is { } interval
        && interval > TimeSpan.Zero
        && interval < LeaseDuration
            ? interval
            : TimeSpan.FromTicks(LeaseDuration.Ticks / 3);
}
