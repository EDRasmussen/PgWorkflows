namespace PgWorkflows.Workflows;

public interface IPgWorkflowClient
{
    ValueTask<TOutput> ExecuteAsync<TWorkflow, TInput, TOutput>(
        TInput input,
        string? idempotencyKey = null,
        CancellationToken cancellationToken = default
    )
        where TWorkflow : class;

    ValueTask<WorkflowHandle<TOutput>> StartAsync<TWorkflow, TInput, TOutput>(
        TInput input,
        string? idempotencyKey = null,
        CancellationToken cancellationToken = default
    )
        where TWorkflow : class;

    ValueTask SignalAsync<TSignal>(
        Guid workflowRunId,
        string name,
        TSignal signal,
        string? idempotencyKey = null,
        CancellationToken cancellationToken = default
    );
}
