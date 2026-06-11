using System.Collections.Concurrent;
using Microsoft.Extensions.Logging;
using PgWorkflows.Persistence;
using PgWorkflows.Workflows;

namespace PgWorkflows.Workers;

internal sealed class WorkflowWorker(
    WorkflowRegistry registry,
    IWorkflowStore store,
    WorkflowRunner runner,
    IServiceProvider serviceProvider,
    WorkflowWorkerOptions? options = null,
    ILogger<WorkflowWorker>? logger = null
)
{
    private readonly WorkflowRegistry _registry =
        registry ?? throw new ArgumentNullException(nameof(registry));
    private readonly IWorkflowStore _store =
        store ?? throw new ArgumentNullException(nameof(store));
    private readonly WorkflowRunner _runner =
        runner ?? throw new ArgumentNullException(nameof(runner));
    private readonly IServiceProvider _serviceProvider =
        serviceProvider ?? throw new ArgumentNullException(nameof(serviceProvider));
    private readonly WorkflowWorkerOptions _options = options ?? new WorkflowWorkerOptions();
    private readonly ILogger<WorkflowWorker>? _logger = logger;

    public async ValueTask<int> RunOnceAsync(CancellationToken cancellationToken = default)
    {
        var leaseCount = Math.Min(
            Math.Max(_options.BatchSize, 1),
            Math.Max(_options.MaxConcurrency, 1)
        );

        var leasedRuns = await _store.LeaseRunsAsync(
            _options.WorkerId,
            leaseCount,
            _options.LeaseDuration,
            DateTimeOffset.UtcNow,
            Math.Max(_options.MaxAttempts, 1),
            cancellationToken
        );

        if (leasedRuns.Count == 0)
        {
            return 0;
        }

        _logger?.LogDebug(
            "Workflow worker {WorkerId} leased {RunCount} workflow runs.",
            _options.WorkerId,
            leasedRuns.Count
        );

        var errors = new ConcurrentBag<Exception>();
        var heartbeat = new LeaseHeartbeat(
            (leases, expiry, ct) => _store.RenewRunLeasesAsync(leases, expiry, ct),
            _options.LeaseDuration,
            _logger
        );
        using var heartbeatCts = CancellationTokenSource.CreateLinkedTokenSource(cancellationToken);
        var heartbeatLoop = heartbeat.RunAsync(heartbeatCts.Token);

        try
        {
            await Parallel.ForEachAsync(
                leasedRuns,
                new ParallelOptions
                {
                    MaxDegreeOfParallelism = Math.Max(_options.MaxConcurrency, 1),
                    CancellationToken = cancellationToken,
                },
                async (leasedRun, ct) =>
                {
                    try
                    {
                        await ExecuteAsync(leasedRun, heartbeat, ct);
                    }
                    catch (OperationCanceledException) when (ct.IsCancellationRequested)
                    {
                        throw;
                    }
                    catch (Exception ex)
                    {
                        errors.Add(ex);
                    }
                }
            );
        }
        finally
        {
            await heartbeatCts.CancelAsync();
            await heartbeatLoop;
        }

        if (!errors.IsEmpty)
        {
            throw new AggregateException("One or more workflow runs failed to execute.", errors);
        }

        return leasedRuns.Count;
    }

    public async Task RunAsync(CancellationToken cancellationToken = default)
    {
        var heartbeat = new LeaseHeartbeat(
            (leases, expiry, ct) => _store.RenewRunLeasesAsync(leases, expiry, ct),
            _options.LeaseDuration,
            _logger
        );
        using var heartbeatCts = CancellationTokenSource.CreateLinkedTokenSource(cancellationToken);
        var heartbeatLoop = heartbeat.RunAsync(heartbeatCts.Token);

        try
        {
            await ContinuousDispatcher.RunAsync(
                new ContinuousDispatcher.Settings(
                    "Workflow",
                    _options.WorkerId,
                    _options.MaxConcurrency,
                    _options.BatchSize,
                    _options.PollInterval
                ),
                (count, ct) =>
                    _store.LeaseRunsAsync(
                        _options.WorkerId,
                        count,
                        _options.LeaseDuration,
                        DateTimeOffset.UtcNow,
                        Math.Max(_options.MaxAttempts, 1),
                        ct
                    ),
                static run => run.WorkflowRunId,
                async (run, ct) =>
                {
                    await ExecuteAsync(run, heartbeat, ct);
                    return true;
                },
                _logger,
                cancellationToken
            );
        }
        finally
        {
            await heartbeatCts.CancelAsync();
            await heartbeatLoop;
        }
    }

    private async Task ExecuteAsync(
        LeasedWorkflowRun leasedRun,
        LeaseHeartbeat heartbeat,
        CancellationToken cancellationToken
    )
    {
        if (!_registry.TryResolve(leasedRun.WorkflowName, out var definition) || definition is null)
        {
            await _store.RecordRunFailureAsync(
                leasedRun.WorkflowRunId,
                $"No workflow type was registered for '{leasedRun.WorkflowName}'.",
                leasedRun.LeaseToken,
                CancellationToken.None
            );
            return;
        }

        using var runCts = CancellationTokenSource.CreateLinkedTokenSource(cancellationToken);
        var leaseLost = false;
        heartbeat.Register(
            leasedRun.WorkflowRunId,
            leasedRun.LeaseToken,
            leasedRun.LeaseExpiresAt,
            onLeaseLost: () =>
            {
                leaseLost = true;
                try
                {
                    runCts.Cancel();
                }
                catch (ObjectDisposedException)
                {
                    // The run already finished and the token was disposed; nothing to stop.
                }
            }
        );

        try
        {
            await _runner.ExecuteLeasedAsync(
                leasedRun.WorkflowRunId,
                definition,
                _serviceProvider,
                leasedRun.LeaseToken,
                runCts.Token
            );
        }
        catch (OperationCanceledException) when (leaseLost)
        {
            _logger?.LogWarning(
                "Abandoning workflow run {WorkflowRunId} ({WorkflowName}) because its lease was lost.",
                leasedRun.WorkflowRunId,
                leasedRun.WorkflowName
            );
        }
        catch (OperationCanceledException) when (cancellationToken.IsCancellationRequested)
        {
            throw;
        }
        catch (Exception ex) when (TransientErrors.IsTransient(ex))
        {
            // The database was unreachable or overloaded; the workflow itself did not fail.
            // Release the run (rolling back the attempt the lease charged) and rethrow so the
            // worker loop backs off instead of hammering an exhausted database.
            await ReleaseRunAfterTransientErrorAsync(leasedRun, ex);
            throw;
        }
        catch (Exception ex)
        {
            await RecordWorkflowFailureAsync(leasedRun, ex, runCts.Token);
        }
        finally
        {
            heartbeat.Unregister(leasedRun.WorkflowRunId);
            runCts.Cancel();
        }
    }

    private async Task ReleaseRunAfterTransientErrorAsync(LeasedWorkflowRun leasedRun, Exception ex)
    {
        var visibleAt = DateTimeOffset.UtcNow.Add(_options.GetRetryDelay(leasedRun.Attempt));
        var released = await _store.ReleaseRunAsync(
            leasedRun.WorkflowRunId,
            leasedRun.LeaseToken,
            visibleAt,
            CancellationToken.None
        );

        if (!released)
        {
            _logger?.LogWarning(
                "Workflow run {WorkflowRunId} ({WorkflowName}) hit a transient infrastructure error but its lease was already lost; another worker will pick it up.",
                leasedRun.WorkflowRunId,
                leasedRun.WorkflowName
            );
            return;
        }

        _logger?.LogWarning(
            ex,
            "Workflow run {WorkflowRunId} ({WorkflowName}) hit a transient infrastructure error; released for retry at {VisibleAt} without consuming an attempt.",
            leasedRun.WorkflowRunId,
            leasedRun.WorkflowName,
            visibleAt
        );
    }

    private async Task RecordWorkflowFailureAsync(
        LeasedWorkflowRun leasedRun,
        Exception ex,
        CancellationToken cancellationToken
    )
    {
        var retryable = leasedRun.Attempt < leasedRun.MaxAttempts;
        var nextVisibleAt = retryable
            ? DateTimeOffset.UtcNow.Add(_options.GetRetryDelay(leasedRun.Attempt))
            : (DateTimeOffset?)null;
        var error = ex.ToString();

        if (!retryable)
        {
            try
            {
                await _runner.RunFailureHooksAsync(leasedRun.WorkflowRunId, cancellationToken);
            }
            catch (OperationCanceledException) when (cancellationToken.IsCancellationRequested)
            {
                throw;
            }
            catch (InvalidOperationException hookEx)
            {
                error =
                    $"{error}{Environment.NewLine}Failure hook error:{Environment.NewLine}{hookEx}";
            }
        }

        var recorded = await _store.RecordRunFailureAsync(
            leasedRun.WorkflowRunId,
            error,
            leasedRun.LeaseToken,
            retryable,
            nextVisibleAt,
            CancellationToken.None
        );

        if (!recorded)
        {
            _logger?.LogWarning(
                "Failure for workflow run {WorkflowRunId} ({WorkflowName}) was not recorded because the lease was lost.",
                leasedRun.WorkflowRunId,
                leasedRun.WorkflowName
            );
            return;
        }

        if (retryable)
        {
            _logger?.LogInformation(
                "Workflow run {WorkflowRunId} ({WorkflowName}) failed on attempt {Attempt}/{MaxAttempts}; retrying at {NextVisibleAt}.",
                leasedRun.WorkflowRunId,
                leasedRun.WorkflowName,
                leasedRun.Attempt,
                leasedRun.MaxAttempts,
                nextVisibleAt
            );
            return;
        }

        _logger?.LogWarning(
            ex,
            "Workflow run {WorkflowRunId} ({WorkflowName}) failed terminally on attempt {Attempt}/{MaxAttempts}.",
            leasedRun.WorkflowRunId,
            leasedRun.WorkflowName,
            leasedRun.Attempt,
            leasedRun.MaxAttempts
        );
    }

}
