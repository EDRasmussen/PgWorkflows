namespace PgWorkflows.Workers;

public sealed class ActivityWorkerOptions
{
    public string WorkerId { get; init; } = Environment.MachineName;

    public int BatchSize { get; init; } = 16;

    public TimeSpan LeaseDuration { get; init; } = TimeSpan.FromSeconds(30);

    public TimeSpan PollInterval { get; init; } = TimeSpan.FromMilliseconds(250);

    public Func<int, TimeSpan> GetRetryDelay { get; init; } =
        static attempt => TimeSpan.FromSeconds(Math.Min(Math.Max(attempt, 1) * 5, 60));
}
