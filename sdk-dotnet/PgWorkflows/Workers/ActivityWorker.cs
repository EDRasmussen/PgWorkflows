using System.Collections.Concurrent;
using PgWorkflows.Activities;
using PgWorkflows.Persistence;

namespace PgWorkflows.Workers;

public sealed class ActivityWorker(
    ActivityRegistry registry,
    IActivityJobStore store,
    ActivityWorkerOptions? options = null
)
{
    private static readonly TimeSpan MaxBackoff = TimeSpan.FromSeconds(30);

    private readonly ActivityRegistry _registry =
        registry ?? throw new ArgumentNullException(nameof(registry));
    private readonly IActivityJobStore _store =
        store ?? throw new ArgumentNullException(nameof(store));
    private readonly ActivityWorkerOptions _options = options ?? new ActivityWorkerOptions();

    public async ValueTask<int> RunOnceAsync(CancellationToken cancellationToken = default)
    {
        // Never lease more than we can run at once: a job that waits behind the
        // concurrency limit has no renewer, so its lease would lapse and another worker
        // would reclaim it. Lease exactly what we can keep alive.
        var leaseCount = Math.Min(
            Math.Max(_options.BatchSize, 1),
            Math.Max(_options.MaxConcurrency, 1)
        );

        var leasedJobs = await _store.LeaseAsync(
            new LeaseActivityJobsRequest(
                _options.WorkerId,
                leaseCount,
                _options.LeaseDuration,
                DateTimeOffset.UtcNow
            ),
            cancellationToken
        );

        if (leasedJobs.Count == 0)
        {
            return 0;
        }

        // A job whose outcome couldn't be recorded (a real store error, distinct from the
        // expected "lease lost" race) is collected rather than thrown from the body —
        // throwing here would cancel sibling jobs in the batch. We surface it after the
        // batch so the caller (RunAsync) can back off, while the job stays leased and is
        // retried after its lease expires.
        var writeErrors = new ConcurrentBag<Exception>();

        await Parallel.ForEachAsync(
            leasedJobs,
            new ParallelOptions
            {
                MaxDegreeOfParallelism = Math.Max(_options.MaxConcurrency, 1),
                CancellationToken = cancellationToken,
            },
            async (leasedJob, ct) =>
            {
                var error = await ExecuteAsync(leasedJob, ct);
                if (error is not null)
                {
                    writeErrors.Add(error);
                }
            }
        );

        if (!writeErrors.IsEmpty)
        {
            throw new AggregateException(
                "One or more job outcomes could not be recorded; those jobs remain leased and will be retried after the lease expires.",
                writeErrors
            );
        }

        return leasedJobs.Count;
    }

    public async Task RunAsync(CancellationToken cancellationToken = default)
    {
        var consecutiveFailures = 0;

        while (!cancellationToken.IsCancellationRequested)
        {
            try
            {
                var processed = await RunOnceAsync(cancellationToken);
                consecutiveFailures = 0;

                if (processed == 0)
                {
                    await Task.Delay(_options.PollInterval, cancellationToken);
                }
            }
            catch (OperationCanceledException) when (cancellationToken.IsCancellationRequested)
            {
                return; // graceful shutdown
            }
            catch (Exception ex)
            {
                // Process-fatal conditions must take the worker (and process) down rather
                // than being retried forever.
                if (ex is OutOfMemoryException)
                {
                    throw;
                }

                // A transient failure (e.g. the store is briefly unavailable) must not kill
                // the worker. Back off — growing with consecutive failures so a sustained
                // outage isn't hammered — and try again. (Phase 1 adds structured logging.)
                consecutiveFailures++;
                try
                {
                    await Task.Delay(BackoffDelay(consecutiveFailures), cancellationToken);
                }
                catch (OperationCanceledException)
                {
                    return;
                }
            }
        }
    }

    private TimeSpan BackoffDelay(int consecutiveFailures)
    {
        var baseMs = _options.PollInterval.TotalMilliseconds;
        var factor = Math.Pow(2, Math.Min(consecutiveFailures - 1, 16));
        return TimeSpan.FromMilliseconds(Math.Min(baseMs * factor, MaxBackoff.TotalMilliseconds));
    }

    // Returns the exception if recording the outcome failed (a real store error), otherwise
    // null. Never propagates: a throw from a Parallel body would cancel sibling jobs.
    private async Task<Exception?> ExecuteAsync(
        LeasedActivityJob leasedJob,
        CancellationToken cancellationToken
    )
    {
        if (!_registry.TryResolve(leasedJob.ActivityName, out var handler) || handler is null)
        {
            return await TryRecordAsync(() =>
                _store.RecordFailureAsync(
                    leasedJob.JobId,
                    leasedJob.LeaseToken,
                    $"No activity handler was registered for '{leasedJob.ActivityName}'.",
                    retryable: false,
                    nextVisibleAt: null,
                    cancellationToken
                )
            );
        }

        // One token ties the handler and its renewer together. Cancelling it both stops
        // the handler and tells the renewer to exit, so it serves two roles: the renewer
        // cancels it when the lease is lost (the handler must stop working on a job it no
        // longer owns), and the finally cancels it once the handler returns (the renewer
        // can stop). leaseLost records which of the two happened.
        using var jobCts = CancellationTokenSource.CreateLinkedTokenSource(cancellationToken);
        var leaseLost = false;

        var renewer = RenewLeaseLoopAsync(
            leasedJob,
            onLeaseLost: () =>
            {
                leaseLost = true;
                jobCts.Cancel();
            },
            stopToken: jobCts.Token
        );

        var context = new ActivityExecutionContext(
            leasedJob.JobId,
            leasedJob.ActivityName,
            leasedJob.Attempt,
            leasedJob.CreatedAt
        );

        string? resultJson = null;
        Exception? handlerError = null;
        try
        {
            resultJson = await handler(context, leasedJob.InputJson, jobCts.Token);
        }
        catch (Exception ex)
        {
            handlerError = ex;
        }
        finally
        {
            jobCts.Cancel();
            await renewer;
        }

        // Lease was reclaimed mid-execution: another worker owns the job now, so write
        // nothing — recording here would clobber that worker.
        if (leaseLost)
        {
            return null;
        }

        // If the handler finished, record the outcome even mid-shutdown — the work is
        // already done and we still hold the lease, so persisting it avoids a needless
        // re-run. Only abandon when shutdown cancelled the handler before it completed.
        var shuttingDown = cancellationToken.IsCancellationRequested;
        if (shuttingDown && handlerError is not null)
        {
            return null;
        }

        // During shutdown the worker's token is already cancelled, so bound the final write
        // with a fresh, time-limited token instead — it must complete the record without
        // being aborted, but must not hang graceful shutdown indefinitely.
        using var writeCts = shuttingDown
            ? new CancellationTokenSource(_options.ShutdownWriteTimeout)
            : null;
        var writeToken = writeCts?.Token ?? cancellationToken;

        if (handlerError is null)
        {
            return await TryRecordAsync(() =>
                _store.RecordSuccessAsync(leasedJob.JobId, leasedJob.LeaseToken, resultJson, writeToken)
            );
        }

        var retryable = leasedJob.Attempt < leasedJob.MaxAttempts;
        DateTimeOffset? nextVisibleAt = retryable
            ? DateTimeOffset.UtcNow + _options.GetRetryDelay(leasedJob.Attempt)
            : null;

        return await TryRecordAsync(() =>
            _store.RecordFailureAsync(
                leasedJob.JobId,
                leasedJob.LeaseToken,
                handlerError.ToString(),
                retryable,
                nextVisibleAt,
                writeToken
            )
        );
    }

    // A false return means the lease was lost (an expected race) — ignored, the new holder
    // records the outcome. A thrown exception means a real store error: it's returned so
    // RunOnceAsync can surface it, rather than being silently swallowed.
    private static async Task<Exception?> TryRecordAsync(Func<ValueTask<bool>> record)
    {
        try
        {
            await record();
            return null;
        }
        catch (Exception ex)
        {
            return ex;
        }
    }

    private async Task RenewLeaseLoopAsync(
        LeasedActivityJob leasedJob,
        Action onLeaseLost,
        CancellationToken stopToken
    )
    {
        var interval = _options.EffectiveRenewalInterval;
        var leaseExpiresAt = leasedJob.LeaseExpiresAt;

        while (true)
        {
            try
            {
                await Task.Delay(interval, stopToken);
            }
            catch (OperationCanceledException)
            {
                // The handler finished (or the worker is shutting down): stop renewing.
                return;
            }

            try
            {
                var newExpiresAt = DateTimeOffset.UtcNow + _options.LeaseDuration;
                if (
                    await _store.RenewLeaseAsync(
                        leasedJob.JobId,
                        leasedJob.LeaseToken,
                        newExpiresAt,
                        stopToken
                    )
                )
                {
                    leaseExpiresAt = newExpiresAt;
                }
                else
                {
                    onLeaseLost();
                    return;
                }
            }
            catch (OperationCanceledException)
            {
                return;
            }
            catch
            {
                // Couldn't renew (transient store error). If the lease has actually lapsed
                // while we were failing, stop the handler — another worker can reclaim the
                // job now and we must not keep running it. Otherwise retry on the next tick.
                if (DateTimeOffset.UtcNow >= leaseExpiresAt)
                {
                    onLeaseLost();
                    return;
                }
            }
        }
    }
}
