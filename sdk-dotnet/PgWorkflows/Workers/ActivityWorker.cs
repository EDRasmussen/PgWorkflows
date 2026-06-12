using System.Collections.Concurrent;
using System.Diagnostics;
using Microsoft.Extensions.Logging;
using PgWorkflows;
using PgWorkflows.Activities;
using PgWorkflows.Persistence;

namespace PgWorkflows.Workers;

internal sealed class ActivityWorker(
    ActivityRegistry registry,
    IActivityJobStore store,
    ActivityWorkerOptions? options = null,
    ILogger<ActivityWorker>? logger = null
)
{
    private readonly ActivityRegistry _registry =
        registry ?? throw new ArgumentNullException(nameof(registry));
    private readonly IActivityJobStore _store =
        store ?? throw new ArgumentNullException(nameof(store));
    private readonly ActivityWorkerOptions _options = options ?? new ActivityWorkerOptions();
    private readonly ILogger<ActivityWorker>? _logger = logger;

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
            _options.WorkerId,
            leaseCount,
            _options.LeaseDuration,
            DateTimeOffset.UtcNow,
            cancellationToken
        );

        if (leasedJobs.Count == 0)
        {
            return 0;
        }

        _logger?.LogDebug(
            "Worker {WorkerId} leased {JobCount} activity jobs.",
            _options.WorkerId,
            leasedJobs.Count
        );

        // A job whose outcome couldn't be recorded (a real store error, distinct from the
        // expected "lease lost" race) is collected rather than thrown from the body —
        // throwing here would cancel sibling jobs in the batch. We surface it after the
        // batch so the caller (RunAsync) can back off, while the job stays leased and is
        // retried after its lease expires.
        var writeErrors = new ConcurrentBag<Exception>();

        await using (
            var heartbeat = LeaseHeartbeat.Start(
                (leases, expiry, ct) => _store.RenewLeasesAsync(leases, expiry, ct),
                _options.LeaseDuration,
                _logger,
                cancellationToken
            )
        )
        {
            await Parallel.ForEachAsync(
                leasedJobs,
                new ParallelOptions
                {
                    MaxDegreeOfParallelism = Math.Max(_options.MaxConcurrency, 1),
                    CancellationToken = cancellationToken,
                },
                async (leasedJob, ct) =>
                {
                    var error = await ExecuteAsync(leasedJob, heartbeat, ct);
                    if (error is not null)
                    {
                        writeErrors.Add(error);
                    }
                }
            );
        }

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
        await using var heartbeat = LeaseHeartbeat.Start(
            (leases, expiry, ct) => _store.RenewLeasesAsync(leases, expiry, ct),
            _options.LeaseDuration,
            _logger,
            cancellationToken
        );

        await ContinuousDispatcher.RunAsync(
            new ContinuousDispatcher.Settings(
                "Activity",
                _options.WorkerId,
                _options.MaxConcurrency,
                _options.BatchSize,
                _options.PollInterval
            ),
            (count, ct) =>
                _store.LeaseAsync(
                    _options.WorkerId,
                    count,
                    _options.LeaseDuration,
                    DateTimeOffset.UtcNow,
                    ct
                ),
            static job => job.JobId,
            async (job, ct) => await ExecuteAsync(job, heartbeat, ct) is null,
            _logger,
            cancellationToken
        );
    }

    // Returns the exception if recording the outcome failed (a real store error), otherwise
    // null. Never propagates: a throw from a Parallel body would cancel sibling jobs.
    private async Task<Exception?> ExecuteAsync(
        LeasedActivityJob leasedJob,
        LeaseHeartbeat heartbeat,
        CancellationToken cancellationToken
    )
    {
        if (!_registry.TryResolve(leasedJob.ActivityName, out var handler) || handler is null)
        {
            _logger?.LogWarning(
                "No activity handler was registered for {ActivityName}; failing job {JobId}.",
                leasedJob.ActivityName,
                leasedJob.JobId
            );
            var record = await TryRecordAsync(() =>
                _store.RecordFailureAsync(
                    leasedJob.JobId,
                    leasedJob.LeaseToken,
                    $"No activity handler was registered for '{leasedJob.ActivityName}'.",
                    retryable: false,
                    nextVisibleAt: null,
                    cancellationToken
                )
            );
            return record.Error;
        }

        // jobCts stops the handler when the lease is lost (the shared heartbeat cancels it — the
        // handler must stop working on a job it no longer owns) or when the handler returns.
        // leaseLost records that the cancellation came from lease loss, not a normal finish.
        using var jobCts = CancellationTokenSource.CreateLinkedTokenSource(cancellationToken);
        var leaseLost = false;

        heartbeat.Register(
            leasedJob.JobId,
            leasedJob.LeaseToken,
            leasedJob.LeaseExpiresAt,
            onLeaseLost: () =>
            {
                leaseLost = true;
                try
                {
                    jobCts.Cancel();
                }
                catch (ObjectDisposedException)
                {
                    // The handler already finished and the token was disposed; nothing to stop.
                }
            }
        );

        var context = new ActivityExecutionContext(
            leasedJob.JobId,
            leasedJob.ActivityName,
            leasedJob.Attempt,
            leasedJob.CreatedAt
        );

        string? resultJson = null;
        Exception? handlerError = null;
        var started = Stopwatch.GetTimestamp();
        using var activity = PgWorkflowsTelemetry.ActivitySource.StartActivity(
            "pgworkflows.activity.execute",
            ActivityKind.Internal
        );
        activity?.SetTag("pgworkflows.worker.id", _options.WorkerId);
        activity?.SetTag("pgworkflows.job.id", leasedJob.JobId);
        activity?.SetTag("pgworkflows.activity.name", leasedJob.ActivityName);
        activity?.SetTag("pgworkflows.activity.attempt", leasedJob.Attempt);
        activity?.SetTag("pgworkflows.activity.max_attempts", leasedJob.MaxAttempts);

        _logger?.LogDebug(
            "Executing activity job {JobId} ({ActivityName}) attempt {Attempt}/{MaxAttempts}.",
            leasedJob.JobId,
            leasedJob.ActivityName,
            leasedJob.Attempt,
            leasedJob.MaxAttempts
        );

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
            heartbeat.Unregister(leasedJob.JobId);
            jobCts.Cancel();
        }

        // Lease was reclaimed mid-execution: another worker owns the job now, so write
        // nothing — recording here would clobber that worker.
        if (leaseLost)
        {
            activity?.SetStatus(ActivityStatusCode.Error, "lease lost");
            _logger?.LogWarning(
                "Abandoning activity job {JobId} ({ActivityName}) because its lease was lost.",
                leasedJob.JobId,
                leasedJob.ActivityName
            );
            return null;
        }

        // If the handler finished, record the outcome even mid-shutdown — the work is
        // already done and we still hold the lease, so persisting it avoids a needless
        // re-run. Only abandon when shutdown cancelled the handler before it completed.
        var shuttingDown = cancellationToken.IsCancellationRequested;
        if (shuttingDown && handlerError is not null)
        {
            activity?.SetStatus(ActivityStatusCode.Error, "shutdown");
            _logger?.LogDebug(
                "Abandoning failed activity job {JobId} ({ActivityName}) during shutdown.",
                leasedJob.JobId,
                leasedJob.ActivityName
            );
            return null;
        }

        // During shutdown the worker's token is already cancelled, so bound the final write
        // with a fresh, time-limited token instead — it must complete the record without
        // being aborted, but must not hang graceful shutdown indefinitely.
        using var writeCts = shuttingDown
            ? new CancellationTokenSource(TimeSpan.FromSeconds(5))
            : null;
        var writeToken = writeCts?.Token ?? cancellationToken;

        if (handlerError is null)
        {
            var record = await TryRecordAsync(() =>
                _store.RecordSuccessAsync(leasedJob.JobId, leasedJob.LeaseToken, resultJson, writeToken)
            );
            if (record is { Error: null, LeaseHeld: true })
            {
                activity?.SetStatus(ActivityStatusCode.Ok);
                _logger?.LogInformation(
                    "Activity job {JobId} ({ActivityName}) succeeded in {ElapsedMs}ms.",
                    leasedJob.JobId,
                    leasedJob.ActivityName,
                    Stopwatch.GetElapsedTime(started).TotalMilliseconds
                );
            }
            else if (record is { Error: null, LeaseHeld: false })
            {
                activity?.SetStatus(ActivityStatusCode.Error, "lease lost");
                _logger?.LogWarning(
                    "Success for activity job {JobId} ({ActivityName}) was not recorded because the lease was lost.",
                    leasedJob.JobId,
                    leasedJob.ActivityName
                );
            }
            else if (record.Error is not null)
            {
                activity?.SetStatus(ActivityStatusCode.Error, "success record failed");
                _logger?.LogError(
                    record.Error,
                    "Failed to record success for activity job {JobId} ({ActivityName}).",
                    leasedJob.JobId,
                    leasedJob.ActivityName
                );
            }

            return record.Error;
        }

        var retryable = leasedJob.Attempt < leasedJob.MaxAttempts;
        DateTimeOffset? nextVisibleAt = retryable
            ? DateTimeOffset.UtcNow + _options.GetRetryDelay(leasedJob.Attempt)
            : null;

        var failureRecord = await TryRecordAsync(() =>
            _store.RecordFailureAsync(
                leasedJob.JobId,
                leasedJob.LeaseToken,
                handlerError.ToString(),
                retryable,
                nextVisibleAt,
                writeToken
            )
        );
        if (failureRecord is { Error: null, LeaseHeld: true })
        {
            activity?.SetStatus(ActivityStatusCode.Error, handlerError.Message);
            activity?.AddException(handlerError);

            if (retryable)
            {
                _logger?.LogWarning(
                    handlerError,
                    "Activity job {JobId} ({ActivityName}) failed on attempt {Attempt}/{MaxAttempts}; retrying at {NextVisibleAt}.",
                    leasedJob.JobId,
                    leasedJob.ActivityName,
                    leasedJob.Attempt,
                    leasedJob.MaxAttempts,
                    nextVisibleAt
                );
            }
            else
            {
                _logger?.LogError(
                    handlerError,
                    "Activity job {JobId} ({ActivityName}) failed terminally on attempt {Attempt}/{MaxAttempts}.",
                    leasedJob.JobId,
                    leasedJob.ActivityName,
                    leasedJob.Attempt,
                    leasedJob.MaxAttempts
                );
            }
        }
        else if (failureRecord is { Error: null, LeaseHeld: false })
        {
            activity?.SetStatus(ActivityStatusCode.Error, "lease lost");
            _logger?.LogWarning(
                "Failure for activity job {JobId} ({ActivityName}) was not recorded because the lease was lost.",
                leasedJob.JobId,
                leasedJob.ActivityName
            );
        }
        else if (failureRecord.Error is not null)
        {
            activity?.SetStatus(ActivityStatusCode.Error, "failure record failed");
            _logger?.LogError(
                failureRecord.Error,
                "Failed to record failure for activity job {JobId} ({ActivityName}).",
                leasedJob.JobId,
                leasedJob.ActivityName
            );
        }

        return failureRecord.Error;
    }

    // A false return means the lease was lost (an expected race) — ignored, the new holder
    // records the outcome. A thrown exception means a real store error: it's returned so
    // RunOnceAsync can surface it, rather than being silently swallowed.
    private static async Task<RecordAttempt> TryRecordAsync(Func<ValueTask<bool>> record)
    {
        try
        {
            return new RecordAttempt(Error: null, LeaseHeld: await record());
        }
        catch (Exception ex)
        {
            return new RecordAttempt(ex, LeaseHeld: false);
        }
    }

    private sealed record RecordAttempt(Exception? Error, bool LeaseHeld);
}
