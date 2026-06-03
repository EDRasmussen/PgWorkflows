namespace PgWorkflows.Workers;

public interface IActivityJobWakeup
{
    ValueTask WaitAsync(TimeSpan timeout, CancellationToken cancellationToken = default);
}
