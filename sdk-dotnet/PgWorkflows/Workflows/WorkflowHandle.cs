namespace PgWorkflows.Workflows;

public sealed class WorkflowHandle<TOutput>
{
    private readonly Func<Guid, CancellationToken, ValueTask<TOutput>> _getResult;

    internal WorkflowHandle(Guid workflowRunId, Func<Guid, CancellationToken, ValueTask<TOutput>> getResult)
    {
        WorkflowRunId = workflowRunId;
        _getResult = getResult ?? throw new ArgumentNullException(nameof(getResult));
    }

    public Guid WorkflowRunId { get; }

    public ValueTask<TOutput> GetResultAsync(CancellationToken cancellationToken = default) =>
        _getResult(WorkflowRunId, cancellationToken);
}
