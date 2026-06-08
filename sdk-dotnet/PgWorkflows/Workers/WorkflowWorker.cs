using System.Collections.Concurrent;
using Microsoft.Extensions.Logging;
using PgWorkflows.Persistence;
using PgWorkflows.Workflows;

namespace PgWorkflows.Workers;

public sealed class WorkflowWorker(
    WorkflowRegistry registry,
    IWorkflowStore store,
    WorkflowRunner runner,
    IServiceProvider serviceProvider,
    WorkflowWorkerOptions? options = null,
    ILogger<WorkflowWorker>? logger = null
)
{
    private static readonly TimeSpan MaxBackoff = TimeSpan.FromSeconds(30);

    private readonly WorkflowRegistry _registry =
        registry ?? throw new ArgumentNullException(nameof(registry));
    private readonly IWorkflowStore _store = store ?? throw new ArgumentNullException(nameof(store));
    private readonly WorkflowRunner _runner = runner ?? throw new ArgumentNullException(nameof(runner));
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
            new LeaseWorkflowRunsRequest(
                _options.WorkerId,
                leaseCount,
                _options.LeaseDuration,
                DateTimeOffset.UtcNow
            ),
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
                    await ExecuteAsync(leasedRun, ct);
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

        if (!errors.IsEmpty)
        {
            throw new AggregateException("One or more workflow runs failed to execute.", errors);
        }

        return leasedRuns.Count;
    }

    public async Task RunAsync(CancellationToken cancellationToken = default)
    {
        var consecutiveFailures = 0;

        _logger?.LogInformation("Workflow worker {WorkerId} started.", _options.WorkerId);

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
                _logger?.LogInformation("Workflow worker {WorkerId} stopped.", _options.WorkerId);
                return;
            }
            catch (Exception ex)
            {
                if (ex is OutOfMemoryException)
                {
                    throw;
                }

                consecutiveFailures++;
                var delay = BackoffDelay(consecutiveFailures);
                _logger?.LogWarning(
                    ex,
                    "Workflow worker {WorkerId} failed; backing off for {BackoffDelayMs}ms before retrying.",
                    _options.WorkerId,
                    delay.TotalMilliseconds
                );

                try
                {
                    await Task.Delay(delay, cancellationToken);
                }
                catch (OperationCanceledException)
                {
                    _logger?.LogInformation("Workflow worker {WorkerId} stopped.", _options.WorkerId);
                    return;
                }
            }
        }

        _logger?.LogInformation("Workflow worker {WorkerId} stopped.", _options.WorkerId);
    }

    private TimeSpan BackoffDelay(int consecutiveFailures)
    {
        var baseMs = _options.PollInterval.TotalMilliseconds;
        var factor = Math.Pow(2, Math.Min(consecutiveFailures - 1, 16));
        return TimeSpan.FromMilliseconds(Math.Min(baseMs * factor, MaxBackoff.TotalMilliseconds));
    }

    private async Task ExecuteAsync(
        LeasedWorkflowRun leasedRun,
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
        var renewer = RenewLeaseLoopAsync(
            leasedRun,
            onLeaseLost: () =>
            {
                leaseLost = true;
                runCts.Cancel();
            },
            stopToken: runCts.Token
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
        finally
        {
            runCts.Cancel();
            await renewer;
        }
    }

    private async Task RenewLeaseLoopAsync(
        LeasedWorkflowRun leasedRun,
        Action onLeaseLost,
        CancellationToken stopToken
    )
    {
        var currentLeaseExpiresAt = leasedRun.LeaseExpiresAt;

        while (!stopToken.IsCancellationRequested)
        {
            try
            {
                await Task.Delay(_options.EffectiveRenewalInterval, stopToken);
                var leaseExpiresAt = DateTimeOffset.UtcNow.Add(_options.LeaseDuration);
                var renewed = await _store.RenewRunLeaseAsync(
                    leasedRun.WorkflowRunId,
                    leasedRun.LeaseToken,
                    leaseExpiresAt,
                    stopToken
                );

                if (!renewed)
                {
                    onLeaseLost();
                    return;
                }

                currentLeaseExpiresAt = leaseExpiresAt;
            }
            catch (OperationCanceledException) when (stopToken.IsCancellationRequested)
            {
                return;
            }
            catch (Exception ex)
            {
                if (DateTimeOffset.UtcNow >= currentLeaseExpiresAt)
                {
                    _logger?.LogWarning(
                        ex,
                        "Workflow run {WorkflowRunId} lease renewal failed past expiry.",
                        leasedRun.WorkflowRunId
                    );
                    onLeaseLost();
                    return;
                }

                _logger?.LogDebug(
                    ex,
                    "Workflow run {WorkflowRunId} lease renewal failed; retrying.",
                    leasedRun.WorkflowRunId
                );
            }
        }
    }
}
