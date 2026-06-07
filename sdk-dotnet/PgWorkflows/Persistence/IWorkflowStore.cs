using PgWorkflows.Workflows;

namespace PgWorkflows.Persistence;

public interface IWorkflowStore
{
    ValueTask<Guid> CreateRunAsync(
        CreateWorkflowRunRequest request,
        CancellationToken cancellationToken = default
    );

    ValueTask<WorkflowRun?> GetRunAsync(Guid workflowRunId, CancellationToken cancellationToken = default);

    ValueTask MarkRunRunningAsync(Guid workflowRunId, CancellationToken cancellationToken = default);

    ValueTask RecordRunSuccessAsync(
        Guid workflowRunId,
        string? resultJson,
        CancellationToken cancellationToken = default
    );

    ValueTask RecordRunFailureAsync(
        Guid workflowRunId,
        string error,
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
}
