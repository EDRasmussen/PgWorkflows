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
        Guid? workflowRunId = null,
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
    /// Extends the lease on many jobs in one statement. Returns the ids still held under their
    /// given lease token; any input id absent from the result has lost its lease (reclaimed or
    /// completed) and its worker should abandon it.
    /// </summary>
    ValueTask<IReadOnlyList<Guid>> RenewLeasesAsync(
        IReadOnlyList<(Guid JobId, string LeaseToken)> leases,
        DateTimeOffset leaseExpiresAt,
        CancellationToken cancellationToken = default
    );

    /// <summary>
    /// Records a successful JSON result. Returns <c>false</c> when the lease is no longer
    /// held (another worker reclaimed the job), in which case nothing was written and
    /// the caller should abandon. When the job belongs to a workflow run and it was the last
    /// incomplete job for that run, the run's parked deadline is pulled forward in the same
    /// transaction so a waiting workflow resumes promptly.
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
