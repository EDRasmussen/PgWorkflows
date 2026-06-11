using System.Collections.Concurrent;
using System.Threading.Channels;
using Microsoft.Extensions.Logging;

namespace PgWorkflows.Workers;

internal static class ContinuousDispatcher
{
    private static readonly TimeSpan MaxBackoff = TimeSpan.FromSeconds(5);

    internal readonly record struct Settings(
        string WorkerKind,
        string WorkerId,
        int MaxConcurrency,
        int BatchSize,
        TimeSpan PollInterval
    );

    public static async Task RunAsync<TItem>(
        Settings settings,
        Func<int, CancellationToken, ValueTask<IReadOnlyList<TItem>>> leaseAsync,
        Func<TItem, Guid> keyOf,
        Func<TItem, CancellationToken, Task<bool>> executeAsync,
        ILogger? logger,
        CancellationToken cancellationToken
    )
    {
        var maxConcurrency = Math.Max(settings.MaxConcurrency, 1);
        var leaseChunk = Math.Min(Math.Max(settings.BatchSize, 1), maxConcurrency);

        using var slots = new SemaphoreSlim(maxConcurrency, maxConcurrency);
        var inFlight = new ConcurrentDictionary<Guid, Task>();
        var outcomes = Channel.CreateUnbounded<bool>(
            new UnboundedChannelOptions { SingleReader = true }
        );
        var leaseFailures = 0;
        var executeFailures = 0;

        logger?.LogInformation(
            "{WorkerKind} worker {WorkerId} started.",
            settings.WorkerKind,
            settings.WorkerId
        );

        try
        {
            while (!cancellationToken.IsCancellationRequested)
            {
                var sawFailure = false;
                var sawSuccess = false;
                while (outcomes.Reader.TryRead(out var ok))
                {
                    if (ok)
                    {
                        sawSuccess = true;
                    }
                    else
                    {
                        sawFailure = true;
                    }
                }

                if (sawSuccess)
                {
                    executeFailures = 0;
                }
                else if (sawFailure)
                {
                    executeFailures++;
                }

                if (executeFailures > 0)
                {
                    await Task.Delay(
                        BackoffDelay(executeFailures, settings.PollInterval),
                        cancellationToken
                    );
                }

                await slots.WaitAsync(cancellationToken);
                var acquired = 1;
                while (acquired < leaseChunk && slots.Wait(0))
                {
                    acquired++;
                }

                IReadOnlyList<TItem> leased;
                try
                {
                    leased = await leaseAsync(acquired, cancellationToken);
                }
                catch (OperationCanceledException) when (cancellationToken.IsCancellationRequested)
                {
                    slots.Release(acquired);
                    break;
                }
                catch (Exception ex)
                {
                    slots.Release(acquired);
                    leaseFailures++;
                    logger?.LogWarning(
                        ex,
                        "{WorkerKind} worker {WorkerId} failed to lease; backing off.",
                        settings.WorkerKind,
                        settings.WorkerId
                    );
                    await Task.Delay(
                        BackoffDelay(leaseFailures, settings.PollInterval),
                        cancellationToken
                    );
                    continue;
                }

                leaseFailures = 0;

                if (leased.Count < acquired)
                {
                    slots.Release(acquired - leased.Count);
                }

                if (leased.Count == 0)
                {
                    await Task.Delay(settings.PollInterval, cancellationToken);
                    continue;
                }

                if (logger?.IsEnabled(LogLevel.Debug) == true)
                {
                    logger.LogDebug(
                        "{WorkerKind} worker {WorkerId} leased {Count} item(s).",
                        settings.WorkerKind,
                        settings.WorkerId,
                        leased.Count
                    );
                }

                foreach (var item in leased)
                {
                    var key = keyOf(item);
                    inFlight[key] = Task.Run(
                        async () =>
                        {
                            var ok = false;
                            try
                            {
                                ok = await executeAsync(item, cancellationToken);
                            }
                            catch (OperationCanceledException)
                                when (cancellationToken.IsCancellationRequested)
                            {
                                ok = true;
                            }
                            catch
                            {
                                ok = false;
                            }
                            finally
                            {
                                // Release before leaving the in-flight map: the drain awaits whatever
                                // is still in the map, so the permit must be returned first or the
                                // drain could dispose the semaphore with this Release still pending.
                                slots.Release();
                                outcomes.Writer.TryWrite(ok);
                                inFlight.TryRemove(key, out _);
                            }
                        },
                        CancellationToken.None
                    );
                }
            }
        }
        catch (OperationCanceledException) when (cancellationToken.IsCancellationRequested) { }
        finally
        {
            await Task.WhenAll([.. inFlight.Values]);
            logger?.LogInformation(
                "{WorkerKind} worker {WorkerId} stopped.",
                settings.WorkerKind,
                settings.WorkerId
            );
        }
    }

    private static TimeSpan BackoffDelay(int consecutiveFailures, TimeSpan pollInterval)
    {
        var baseMs = pollInterval.TotalMilliseconds;
        var factor = Math.Pow(2, Math.Min(consecutiveFailures - 1, 16));
        return TimeSpan.FromMilliseconds(Math.Min(baseMs * factor, MaxBackoff.TotalMilliseconds));
    }
}
