using System.Collections.Concurrent;
using System.Diagnostics;
using Microsoft.Extensions.Logging;
using PgWorkflows;
using PgWorkflows.Activities;
using PgWorkflows.Persistence;

namespace PgWorkflows.Workers;

public sealed class ActivityWorker(
    ActivityRegistry registry,
    IActivityJobStore store,
    ActivityWorkerOptions? options = null,
    ILogger<ActivityWorker>? logger = null,
    IActivityJobWakeup? wakeup = null
)
{
    private static readonly TimeSpan MaxBackoff = TimeSpan.FromSeconds(30);

    private readonly ActivityRegistry _registry =
        registry ?? throw new ArgumentNullException(nameof(registry));
    private readonly IActivityJobStore _store =
        store ?? throw new ArgumentNullException(nameof(store));
    private readonly ActivityWorkerOptions _options = options ?? new ActivityWorkerOptions();
    private readonly ILogger<ActivityWorker>? _logger = logger;
    private readonly IActivityJobWakeup? _wakeup = wakeup;

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

        _logger?.LogDebug(
            "Worker {WorkerId} leased {JobCount} activity jobs.",
            _options.WorkerId,
            leasedJobs.Count
        );
        PgWorkflowsTelemetry.LeasedJobs.Add(leasedJobs.Count, WorkerTag);

        foreach (var leasedJob in leasedJobs)
        {
            PgWorkflowsTelemetry.LeaseAge.Record(
                Math.Max(0, (DateTimeOffset.UtcNow - leasedJob.CreatedAt).TotalMilliseconds),
                ActivityTags(leasedJob)
            );
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

        _logger?.LogInformation("Activity worker {WorkerId} started.", _options.WorkerId);

        while (!cancellationToken.IsCancellationRequested)
        {
            try
            {
                var processed = await RunOnceAsync(cancellationToken);
                consecutiveFailures = 0;

                if (processed == 0)
                {
                    if (_wakeup is null)
                    {
                        await Task.Delay(_options.PollInterval, cancellationToken);
                    }
                    else
                    {
                        await _wakeup.WaitAsync(_options.PollInterval, cancellationToken);
                    }
                }
            }
            catch (OperationCanceledException) when (cancellationToken.IsCancellationRequested)
            {
                _logger?.LogInformation("Activity worker {WorkerId} stopped.", _options.WorkerId);
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
                var delay = BackoffDelay(consecutiveFailures);
                _logger?.LogWarning(
                    ex,
                    "Activity worker {WorkerId} failed; backing off for {BackoffDelayMs}ms before retrying.",
                    _options.WorkerId,
                    delay.TotalMilliseconds
                );
                PgWorkflowsTelemetry.WorkerFailures.Add(1, WorkerTag);
                try
                {
                    await Task.Delay(delay, cancellationToken);
                }
                catch (OperationCanceledException)
                {
                    _logger?.LogInformation("Activity worker {WorkerId} stopped.", _options.WorkerId);
                    return;
                }
            }
        }

        _logger?.LogInformation("Activity worker {WorkerId} stopped.", _options.WorkerId);
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
            jobCts.Cancel();
            await renewer;
        }

        // Lease was reclaimed mid-execution: another worker owns the job now, so write
        // nothing — recording here would clobber that worker.
        if (leaseLost)
        {
            activity?.SetStatus(ActivityStatusCode.Error, "lease lost");
            RecordExecutionMetrics(leasedJob, started, "lease_lost");
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
            RecordExecutionMetrics(leasedJob, started, "shutdown_abandoned");
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
            ? new CancellationTokenSource(_options.ShutdownWriteTimeout)
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
                RecordExecutionMetrics(leasedJob, started, "succeeded");
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
                RecordExecutionMetrics(leasedJob, started, "lease_lost");
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
            var outcome = retryable ? "failed_retrying" : "failed_terminal";
            activity?.SetStatus(ActivityStatusCode.Error, handlerError.Message);
            activity?.AddException(handlerError);
            RecordExecutionMetrics(leasedJob, started, outcome);

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
            RecordExecutionMetrics(leasedJob, started, "lease_lost");
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
                    _logger?.LogWarning(
                        "Lease renewal lost for activity job {JobId} ({ActivityName}).",
                        leasedJob.JobId,
                        leasedJob.ActivityName
                    );
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
                    _logger?.LogWarning(
                        "Lease renewal failed past expiry for activity job {JobId} ({ActivityName}).",
                        leasedJob.JobId,
                        leasedJob.ActivityName
                    );
                    onLeaseLost();
                    return;
                }
            }
        }
    }

    private KeyValuePair<string, object?> WorkerTag =>
        new("worker.id", _options.WorkerId);

    private KeyValuePair<string, object?>[] ActivityTags(LeasedActivityJob job) =>
        [
            WorkerTag,
            new("activity.name", job.ActivityName),
            new("attempt", job.Attempt),
        ];

    private void RecordExecutionMetrics(LeasedActivityJob job, long started, string outcome)
    {
        var tags = ActivityTags(job).Append(new KeyValuePair<string, object?>("outcome", outcome)).ToArray();
        PgWorkflowsTelemetry.Executions.Add(1, tags);
        PgWorkflowsTelemetry.ExecutionDuration.Record(
            Stopwatch.GetElapsedTime(started).TotalMilliseconds,
            tags
        );
    }

    private sealed record RecordAttempt(Exception? Error, bool LeaseHeld);
}
