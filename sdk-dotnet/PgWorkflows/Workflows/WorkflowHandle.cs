namespace PgWorkflows.Workflows;

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

    public Guid WorkflowRunId { get; }

    public ValueTask<TOutput> GetResultAsync(CancellationToken cancellationToken = default) =>
        _getResult(WorkflowRunId, cancellationToken);

    public ValueTask SignalAsync<TSignal>(
        string name,
        TSignal signal,
        string? idempotencyKey = null,
        CancellationToken cancellationToken = default
    ) =>
        _runner.SignalAsync(WorkflowRunId, name, signal, idempotencyKey, cancellationToken);
}
