using PgWorkflows.Jobs;

namespace PgWorkflows.Persistence;

public interface IActivityJobStore
{
    ValueTask<Guid> EnqueueAsync(
        EnqueueActivityRequest request,
        CancellationToken cancellationToken = default
    );

    ValueTask<IReadOnlyList<LeasedActivityJob>> LeaseAsync(
        LeaseActivityJobsRequest request,
        CancellationToken cancellationToken = default
    );

    ValueTask<ActivityJob?> GetAsync(Guid jobId, CancellationToken cancellationToken = default);

    ValueTask RecordSuccessAsync(
        Guid jobId,
        string leaseToken,
        string? result,
        CancellationToken cancellationToken = default
    );

    ValueTask RecordFailureAsync(
        Guid jobId,
        string leaseToken,
        string error,
        bool retryable,
        DateTimeOffset? nextVisibleAt,
        CancellationToken cancellationToken = default
    );
}
