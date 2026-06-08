using PgWorkflows.Jobs;

namespace PgWorkflows.Persistence;

internal interface IActivityJobStore
{
    ValueTask<Guid> EnqueueAsync(
        string activityName,
        string? inputJson,
        int maxAttempts = 1,
        DateTimeOffset? visibleAt = null,
        string? idempotencyKey = null,
        CancellationToken cancellationToken = default
    );

    ValueTask<IReadOnlyList<LeasedActivityJob>> LeaseAsync(
        string workerId,
        int batchSize,
        TimeSpan leaseDuration,
        DateTimeOffset now,
        CancellationToken cancellationToken = default
    );

    ValueTask<ActivityJob?> GetAsync(Guid jobId, CancellationToken cancellationToken = default);

    /// <summary>
    /// Extends the lease on a job currently held under <paramref name="leaseToken"/>.
    /// Returns <c>false</c> when the lease is no longer held (expired and reclaimed, or
    /// the job already completed), signalling the caller to abandon its work.
    /// </summary>
    ValueTask<bool> RenewLeaseAsync(
        Guid jobId,
        string leaseToken,
        DateTimeOffset leaseExpiresAt,
        CancellationToken cancellationToken = default
    );

    /// <summary>
    /// Records a successful JSON result. Returns <c>false</c> when the lease is no longer
    /// held (another worker reclaimed the job), in which case nothing was written and
    /// the caller should abandon.
    /// </summary>
    ValueTask<bool> RecordSuccessAsync(
        Guid jobId,
        string leaseToken,
        string? resultJson,
        CancellationToken cancellationToken = default
    );

    /// <summary>
    /// Records a failure (retry or terminal). Returns <c>false</c> when the lease is no
    /// longer held, in which case nothing was written and the caller should abandon.
    /// </summary>
    ValueTask<bool> RecordFailureAsync(
        Guid jobId,
        string leaseToken,
        string error,
        bool retryable,
        DateTimeOffset? nextVisibleAt,
        CancellationToken cancellationToken = default
    );
}
