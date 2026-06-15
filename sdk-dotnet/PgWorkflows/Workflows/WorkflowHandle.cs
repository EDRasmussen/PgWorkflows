namespace PgWorkflows.Workflows;

/// <summary>
/// A reference to a started workflow run: carries its id, awaits its durable result, and
/// delivers signals to it. Handles are cheap and stateless — the run itself lives in Postgres,
/// so a handle can be discarded and the run looked up later by id from any process.
/// </summary>
public sealed class WorkflowHandle<TOutput>
{
    private readonly Func<Guid, CancellationToken, ValueTask<TOutput>> _getResult;
    private readonly WorkflowRunner _runner;

    internal WorkflowHandle(
        Guid workflowRunId,
        Func<Guid, CancellationToken, ValueTask<TOutput>> getResult,
        WorkflowRunner runner
    )
    {
        WorkflowRunId = workflowRunId;
        _getResult = getResult ?? throw new ArgumentNullException(nameof(getResult));
        _runner = runner ?? throw new ArgumentNullException(nameof(runner));
    }

    /// <summary>The run's id in <c>pw_workflow_runs</c>.</summary>
    public Guid WorkflowRunId { get; }

    /// <summary>
    /// Waits until a worker drives the run to completion and returns its recorded result.
    /// Throws <see cref="InvalidOperationException"/> if the run failed terminally.
    /// </summary>
    public ValueTask<TOutput> GetResultAsync(CancellationToken cancellationToken = default) =>
        _getResult(WorkflowRunId, cancellationToken);

    /// <summary>
    /// Delivers a signal to this run. Signals are buffered until the workflow consumes them with
    /// <see cref="IWorkflowContext.WaitForSignal{TSignal}"/>; an optional
    /// <paramref name="idempotencyKey"/> dedupes redelivery.
    /// </summary>
    public ValueTask SignalAsync<TSignal>(
        string name,
        TSignal signal,
        string? idempotencyKey = null,
        CancellationToken cancellationToken = default
    ) => _runner.SignalAsync(WorkflowRunId, name, signal, idempotencyKey, cancellationToken);
}
