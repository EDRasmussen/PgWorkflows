using PgWorkflows.Workflows;

namespace PgWorkflows.Persistence;

internal interface IWorkflowStore
{
    ValueTask<Guid> CreateRunAsync(
        CreateWorkflowRunRequest request,
        CancellationToken cancellationToken = default
    );

    ValueTask<WorkflowRun?> GetRunAsync(Guid workflowRunId, CancellationToken cancellationToken = default);

    ValueTask<IReadOnlyList<LeasedWorkflowRun>> LeaseRunsAsync(
        string workerId,
        int limit,
        TimeSpan leaseDuration,
        DateTimeOffset now,
        int maxAttempts = 1,
        CancellationToken cancellationToken = default
    );

    ValueTask MarkRunRunningAsync(Guid workflowRunId, CancellationToken cancellationToken = default);

    ValueTask<bool> RenewRunLeaseAsync(
        Guid workflowRunId,
        string leaseToken,
        DateTimeOffset leaseExpiresAt,
        CancellationToken cancellationToken = default
    );

    /// <summary>
    /// Extends the lease on many runs in one statement. Returns the ids still held under their
    /// given lease token; any input id absent from the result has lost its lease and its worker
    /// should abandon it.
    /// </summary>
    ValueTask<IReadOnlyList<Guid>> RenewRunLeasesAsync(
        IReadOnlyList<(Guid WorkflowRunId, string LeaseToken)> leases,
        DateTimeOffset leaseExpiresAt,
        CancellationToken cancellationToken = default
    );

    /// <summary>
    /// Releases a leased run back to pending without recording a failure, rolling back the
    /// attempt the lease charged. Used when execution was stopped by a transient infrastructure
    /// error (connection exhaustion, network drop): the workflow did not fail, the worker just
    /// could not reach the database, so the retry must not burn an attempt. The run becomes
    /// visible again at <paramref name="visibleAt"/>. Returns false when the lease was lost; in
    /// that case nothing is written.
    /// </summary>
    ValueTask<bool> ReleaseRunAsync(
        Guid workflowRunId,
        string leaseToken,
        DateTimeOffset visibleAt,
        CancellationToken cancellationToken = default
    );

    ValueTask RecordRunSuccessAsync(
        Guid workflowRunId,
        string? resultJson,
        CancellationToken cancellationToken = default
    );

    ValueTask<bool> RecordRunSuccessAsync(
        Guid workflowRunId,
        string? resultJson,
        string leaseToken,
        CancellationToken cancellationToken = default
    );

    /// <summary>
    /// Parks a leased run until <paramref name="fireAt"/> by reusing the pending/visible_at
    /// machinery, releasing the lease without counting the resume as a failed attempt. The timer's
    /// deadline is persisted in the same transaction so the park and the durable deadline are
    /// atomic. Returns false when the lease was lost (the caller should abandon, not treat the run
    /// as parked); in that case nothing is written.
    /// </summary>
    ValueTask<bool> RecordRunSleepingAsync(
        Guid workflowRunId,
        int timerSequence,
        DateTimeOffset fireAt,
        string leaseToken,
        CancellationToken cancellationToken = default
    );

    /// <summary>
    /// Parks a leased run that is waiting on outstanding activity steps, reusing the
    /// pending/visible_at machinery and releasing the lease without counting the resume as a failed
    /// attempt. Unlike <see cref="RecordRunSleepingAsync"/> there is no timer: the run is normally
    /// woken by the edge-trigger when its last outstanding activity job completes, so it is parked
    /// only until <paramref name="grace"/> elapses as a safety net against a missed wake. If no
    /// activity job for the run is still incomplete at park time (the jobs finished during the tiny
    /// schedule→park window), the run is made immediately runnable instead, closing that race.
    /// Returns false when the lease was lost (the caller should abandon); in that case nothing is
    /// written.
    /// </summary>
    ValueTask<bool> RecordRunWaitingAsync(
        Guid workflowRunId,
        string leaseToken,
        TimeSpan grace,
        CancellationToken cancellationToken = default
    );

    /// <summary>
    /// Parks a leased run that is waiting for an external signal. The park is open-ended (no
    /// deadline): signal delivery wakes the run via the edge-trigger in
    /// <see cref="RecordSignalAsync"/>, and a matching unconsumed signal that arrived during the
    /// consume→park window makes the run immediately runnable instead, closing the lost-wake race.
    /// Returns false when the lease was lost; in that case nothing is written.
    /// </summary>
    ValueTask<bool> RecordRunWaitingForSignalAsync(
        Guid workflowRunId,
        int waitSequence,
        string signalName,
        string leaseToken,
        CancellationToken cancellationToken = default
    );

    ValueTask RecordRunFailureAsync(
        Guid workflowRunId,
        string error,
        CancellationToken cancellationToken = default
    );

    ValueTask<bool> RecordRunFailureAsync(
        Guid workflowRunId,
        string error,
        string leaseToken,
        CancellationToken cancellationToken = default
    );

    ValueTask<bool> RecordRunFailureAsync(
        Guid workflowRunId,
        string error,
        string leaseToken,
        bool retryable,
        DateTimeOffset? nextVisibleAt,
        CancellationToken cancellationToken = default
    );

    /// <summary>
    /// Returns the durable fire time recorded for a workflow timer, or null when the timer has
    /// not been created yet (first encounter). The deadline is persisted atomically with the park
    /// in <see cref="RecordRunSleepingAsync"/>, which keeps <c>ctx.Sleep</c> deterministic across
    /// replays.
    /// </summary>
    ValueTask<DateTimeOffset?> GetTimerAsync(
        Guid workflowRunId,
        int timerSequence,
        CancellationToken cancellationToken = default
    );

    /// <summary>
    /// Claims the oldest unconsumed signal with the given name for the given wait (or replays the
    /// signal already recorded for that wait) and returns its payload. The run row is locked and
    /// the lease verified first, which serializes consumption against signal delivery and fences
    /// out a worker whose lease was taken over. Returns null when no signal is buffered or the
    /// lease was lost; in the latter case nothing is written.
    /// </summary>
    ValueTask<string?> ConsumeSignalAsync(
        Guid workflowRunId,
        int waitSequence,
        string signalName,
        string leaseToken,
        CancellationToken cancellationToken = default
    );

    /// <summary>
    /// Buffers a signal for a run and wakes the run if it is parked waiting on that signal name.
    /// Duplicate deliveries (same <paramref name="idempotencyKey"/>) buffer nothing and do not
    /// wake the run. Throws when the run does not exist or has already completed.
    /// </summary>
    ValueTask<Guid> RecordSignalAsync(
        Guid workflowRunId,
        string signalName,
        string payloadJson,
        string? idempotencyKey = null,
        CancellationToken cancellationToken = default
    );

    ValueTask<WorkflowStep?> GetStepAsync(
        Guid workflowRunId,
        int stepSequence,
        CancellationToken cancellationToken = default
    );

    ValueTask RecordStepScheduledAsync(
        Guid workflowRunId,
        int stepSequence,
        string activityName,
        Guid activityJobId,
        string? inputJson,
        CancellationToken cancellationToken = default
    );

    ValueTask RecordStepSuccessAsync(
        Guid workflowRunId,
        int stepSequence,
        string? resultJson,
        CancellationToken cancellationToken = default
    );

    ValueTask RecordStepFailureAsync(
        Guid workflowRunId,
        int stepSequence,
        string error,
        CancellationToken cancellationToken = default
    );

    ValueTask<IReadOnlyList<WorkflowFailureHook>> ListFailureHooksAsync(
        Guid workflowRunId,
        CancellationToken cancellationToken = default
    );

    ValueTask RecordFailureHookRegisteredAsync(
        Guid workflowRunId,
        int hookSequence,
        string activityName,
        string? inputJson,
        CancellationToken cancellationToken = default
    );

    ValueTask RecordFailureHookScheduledAsync(
        Guid workflowRunId,
        int hookSequence,
        Guid activityJobId,
        CancellationToken cancellationToken = default
    );

    ValueTask RecordFailureHookSuccessAsync(
        Guid workflowRunId,
        int hookSequence,
        string? resultJson,
        CancellationToken cancellationToken = default
    );

    ValueTask RecordFailureHookFailureAsync(
        Guid workflowRunId,
        int hookSequence,
        string error,
        CancellationToken cancellationToken = default
    );
}
