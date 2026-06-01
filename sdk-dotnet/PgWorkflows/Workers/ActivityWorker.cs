using PgWorkflows.Activities;
using PgWorkflows.Persistence;

namespace PgWorkflows.Workers;

public sealed class ActivityWorker(
    ActivityRegistry registry,
    IActivityJobStore store,
    ActivityWorkerOptions? options = null
)
{
    private readonly ActivityRegistry _registry =
        registry ?? throw new ArgumentNullException(nameof(registry));
    private readonly IActivityJobStore _store =
        store ?? throw new ArgumentNullException(nameof(store));
    private readonly ActivityWorkerOptions _options = options ?? new ActivityWorkerOptions();

    public async ValueTask<int> RunOnceAsync(CancellationToken cancellationToken = default)
    {
        var leasedJobs = await _store.LeaseAsync(
            new LeaseActivityJobsRequest(
                _options.WorkerId,
                _options.BatchSize,
                _options.LeaseDuration,
                DateTimeOffset.UtcNow
            ),
            cancellationToken
        );

        foreach (var leasedJob in leasedJobs)
        {
            await ExecuteAsync(leasedJob, cancellationToken);
        }

        return leasedJobs.Count;
    }

    public async Task RunAsync(CancellationToken cancellationToken = default)
    {
        while (!cancellationToken.IsCancellationRequested)
        {
            var processed = await RunOnceAsync(cancellationToken);
            if (processed == 0)
            {
                await Task.Delay(_options.PollInterval, cancellationToken);
            }
        }
    }

    private async Task ExecuteAsync(
        LeasedActivityJob leasedJob,
        CancellationToken cancellationToken
    )
    {
        if (!_registry.TryResolve(leasedJob.ActivityName, out var handler) || handler is null)
        {
            await _store.RecordFailureAsync(
                leasedJob.JobId,
                leasedJob.LeaseToken,
                $"No activity handler was registered for '{leasedJob.ActivityName}'.",
                retryable: false,
                nextVisibleAt: null,
                cancellationToken
            );

            return;
        }

        var context = new ActivityExecutionContext(
            leasedJob.JobId,
            leasedJob.ActivityName,
            leasedJob.Attempt,
            leasedJob.CreatedAt
        );

        try
        {
            var result = await handler(context, leasedJob.Input, cancellationToken);
            await _store.RecordSuccessAsync(
                leasedJob.JobId,
                leasedJob.LeaseToken,
                result,
                cancellationToken
            );
        }
        catch (OperationCanceledException) when (cancellationToken.IsCancellationRequested)
        {
            throw;
        }
        catch (Exception ex)
        {
            var retryable = leasedJob.Attempt < leasedJob.MaxAttempts;
            DateTimeOffset? nextVisibleAt = retryable
                ? DateTimeOffset.UtcNow + _options.GetRetryDelay(leasedJob.Attempt)
                : null;

            await _store.RecordFailureAsync(
                leasedJob.JobId,
                leasedJob.LeaseToken,
                ex.ToString(),
                retryable,
                nextVisibleAt,
                cancellationToken
            );
        }
    }
}
