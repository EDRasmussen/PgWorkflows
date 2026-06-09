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
