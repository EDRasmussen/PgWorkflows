using System.Collections.Concurrent;
using Microsoft.Extensions.Logging;

namespace PgWorkflows.Workers;

internal sealed class LeaseHeartbeat : IAsyncDisposable
{
    private sealed class Entry(string leaseToken, Action onLeaseLost)
    {
        public string LeaseToken { get; } = leaseToken;
        public Action OnLeaseLost { get; } = onLeaseLost;
        public DateTimeOffset Expiry { get; set; }
    }

    private readonly ConcurrentDictionary<Guid, Entry> _entries = new();
    private readonly Func<
        IReadOnlyList<(Guid Id, string LeaseToken)>,
        DateTimeOffset,
        CancellationToken,
        ValueTask<IReadOnlyList<Guid>>
    > _renew;
    private readonly TimeSpan _leaseDuration;
    private readonly TimeSpan _interval;
    private readonly ILogger? _logger;
    private CancellationTokenSource? _loopCts;
    private Task? _loop;

    /// <summary>
    /// Creates a heartbeat and starts its renewal loop, linked to
    /// <paramref name="cancellationToken"/>. Disposal cancels the loop and awaits its drain, so
    /// callers hold the heartbeat in an <c>await using</c> for the duration of the leased work.
    /// </summary>
    public static LeaseHeartbeat Start(
        Func<
            IReadOnlyList<(Guid Id, string LeaseToken)>,
            DateTimeOffset,
            CancellationToken,
            ValueTask<IReadOnlyList<Guid>>
        > renew,
        TimeSpan leaseDuration,
        ILogger? logger,
        CancellationToken cancellationToken
    )
    {
        var heartbeat = new LeaseHeartbeat(renew, leaseDuration, logger);
        heartbeat._loopCts = CancellationTokenSource.CreateLinkedTokenSource(cancellationToken);
        heartbeat._loop = heartbeat.RunAsync(heartbeat._loopCts.Token);
        return heartbeat;
    }

    public async ValueTask DisposeAsync()
    {
        if (_loopCts is null || _loop is null)
        {
            return;
        }

        await _loopCts.CancelAsync();
        await _loop;
        _loopCts.Dispose();
    }

    private LeaseHeartbeat(
        Func<
            IReadOnlyList<(Guid Id, string LeaseToken)>,
            DateTimeOffset,
            CancellationToken,
            ValueTask<IReadOnlyList<Guid>>
        > renew,
        TimeSpan leaseDuration,
        ILogger? logger
    )
    {
        _renew = renew;
        _leaseDuration = leaseDuration;
        _interval = TimeSpan.FromTicks(
            Math.Max(leaseDuration.Ticks / 3, TimeSpan.FromMilliseconds(1).Ticks)
        );
        _logger = logger;
    }

    public void Register(
        Guid key,
        string leaseToken,
        DateTimeOffset leaseExpiresAt,
        Action onLeaseLost
    ) => _entries[key] = new Entry(leaseToken, onLeaseLost) { Expiry = leaseExpiresAt };

    public void Unregister(Guid key) => _entries.TryRemove(key, out _);

    private async Task RunAsync(CancellationToken cancellationToken)
    {
        while (!cancellationToken.IsCancellationRequested)
        {
            try
            {
                await Task.Delay(_interval, cancellationToken);
            }
            catch (OperationCanceledException)
            {
                return;
            }

            var snapshot = _entries.ToArray();
            if (snapshot.Length == 0)
            {
                continue;
            }

            var now = DateTimeOffset.UtcNow;
            var newExpiry = now + _leaseDuration;
            var leases = new (Guid, string)[snapshot.Length];
            for (var i = 0; i < snapshot.Length; i++)
            {
                leases[i] = (snapshot[i].Key, snapshot[i].Value.LeaseToken);
            }

            IReadOnlyList<Guid> held;
            try
            {
                held = await _renew(leases, newExpiry, cancellationToken);
            }
            catch (OperationCanceledException) when (cancellationToken.IsCancellationRequested)
            {
                return;
            }
            catch (Exception ex)
            {
                // Transient store error this tick. A failed query proves nothing about who holds a
                // lease, so only drop items whose lease has actually lapsed.
                _logger?.LogWarning(ex, "Lease heartbeat renewal failed; retrying next tick.");
                foreach (var (key, entry) in snapshot)
                {
                    if (now >= entry.Expiry)
                    {
                        LoseLease(key);
                    }
                }

                continue;
            }

            var heldSet = held.ToHashSet();
            foreach (var (key, entry) in snapshot)
            {
                if (heldSet.Contains(key))
                {
                    entry.Expiry = newExpiry;
                }
                else
                {
                    LoseLease(key);
                }
            }
        }
    }

    private void LoseLease(Guid key)
    {
        // Guarded remove: whoever removes the entry first wins, so a completing item that already
        // unregistered is never declared lost, and onLeaseLost fires at most once.
        if (!_entries.TryRemove(key, out var entry))
        {
            return;
        }

        // A throwing callback must not take down the loop — that would stop renewing every other
        // lease this worker holds.
        try
        {
            entry.OnLeaseLost();
        }
        catch (Exception ex)
        {
            _logger?.LogWarning(ex, "Lease-lost handler threw; heartbeat continuing.");
        }
    }
}
