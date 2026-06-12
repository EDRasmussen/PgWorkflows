namespace PgWorkflows.Workflows;

/// <summary>
/// The application-facing entry point for workflow runs: start a run and await its result, or
/// fire-and-track with a <see cref="WorkflowHandle{TOutput}"/>. Resolved from DI after
/// <c>AddPgWorkflows</c>; in a <c>DisableWorkers()</c> process the client only enqueues — a
/// worker process pointed at the same database executes the run.
/// </summary>
public interface IPgWorkflowClient
{
    /// <summary>
    /// Starts a workflow run and waits for its durable result. Equivalent to
    /// <see cref="StartAsync{TWorkflow, TInput, TOutput}"/> followed by
    /// <see cref="WorkflowHandle{TOutput}.GetResultAsync"/>.
    /// </summary>
    ValueTask<TOutput> ExecuteAsync<TWorkflow, TInput, TOutput>(
        TInput input,
        string? idempotencyKey = null,
        CancellationToken cancellationToken = default
    )
        where TWorkflow : class;

    /// <summary>
    /// Durably creates a workflow run and returns a handle to it without waiting for execution.
    /// With an <paramref name="idempotencyKey"/>, a retried start returns the existing run
    /// instead of creating a duplicate.
    /// </summary>
    ValueTask<WorkflowHandle<TOutput>> StartAsync<TWorkflow, TInput, TOutput>(
        TInput input,
        string? idempotencyKey = null,
        CancellationToken cancellationToken = default
    )
        where TWorkflow : class;

    /// <summary>
    /// Delivers a signal to a run by id. Signals are buffered until the workflow consumes them
    /// with <see cref="IWorkflowContext.WaitForSignal{TSignal}"/>; an optional
    /// <paramref name="idempotencyKey"/> dedupes redelivery. Throws if the run does not exist or
    /// has already completed.
    /// </summary>
    ValueTask SignalAsync<TSignal>(
        Guid workflowRunId,
        string name,
        TSignal signal,
        string? idempotencyKey = null,
        CancellationToken cancellationToken = default
    );
}
