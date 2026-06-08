using PgWorkflows.Workflows;

namespace PgWorkflows.Persistence;

public interface IWorkflowStore
{
    ValueTask<Guid> CreateRunAsync(
        CreateWorkflowRunRequest request,
        CancellationToken cancellationToken = default
    );

    ValueTask<WorkflowRun?> GetRunAsync(Guid workflowRunId, CancellationToken cancellationToken = default);

    ValueTask<IReadOnlyList<LeasedWorkflowRun>> LeaseRunsAsync(
        LeaseWorkflowRunsRequest request,
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

    ValueTask<WorkflowStep?> GetStepAsync(
        Guid workflowRunId,
        int stepSequence,
        CancellationToken cancellationToken = default
    );

    ValueTask RecordStepScheduledAsync(
        RecordWorkflowStepRequest request,
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
        RecordWorkflowFailureHookRequest request,
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
