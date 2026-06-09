namespace PgWorkflows.Workflows;

internal sealed class PgWorkflowClient(
    WorkflowRegistry registry,
    WorkflowRunner runner,
    IServiceProvider serviceProvider,
    bool executeWorkflowsInCaller = true
) : IPgWorkflowClient
{
    private readonly WorkflowRegistry _registry = registry ?? throw new ArgumentNullException(nameof(registry));
    private readonly WorkflowRunner _runner = runner ?? throw new ArgumentNullException(nameof(runner));
    private readonly IServiceProvider _serviceProvider =
        serviceProvider ?? throw new ArgumentNullException(nameof(serviceProvider));
    private readonly bool _executeWorkflowsInCaller = executeWorkflowsInCaller;

    public async ValueTask<TOutput> ExecuteAsync<TWorkflow, TInput, TOutput>(
        TInput input,
        string? idempotencyKey = null,
        CancellationToken cancellationToken = default
    )
        where TWorkflow : class
    {
        var handle = await StartAsync<TWorkflow, TInput, TOutput>(
            input,
            idempotencyKey,
            cancellationToken
        );
        return await handle.GetResultAsync(cancellationToken);
    }

    public async ValueTask<WorkflowHandle<TOutput>> StartAsync<TWorkflow, TInput, TOutput>(
        TInput input,
        string? idempotencyKey = null,
        CancellationToken cancellationToken = default
    )
        where TWorkflow : class
    {
        var definition = _registry.Resolve<TWorkflow>();
        EnsureType<TInput>(definition.InputType, "input", typeof(TWorkflow));
        EnsureType<TOutput>(definition.OutputType, "output", typeof(TWorkflow));

        var workflowRunId = await _runner.StartAsync(
            definition.Name,
            input,
            definition.InputType,
            idempotencyKey,
            cancellationToken
        );

        return new WorkflowHandle<TOutput>(
            workflowRunId,
            (runId, ct) => _executeWorkflowsInCaller
                ? _runner.ExecuteAsync<TOutput>(runId, definition, _serviceProvider, ct)
                : _runner.WaitForResultAsync<TOutput>(runId, ct),
            _runner
        );
    }

    public ValueTask SignalAsync<TSignal>(
        Guid workflowRunId,
        string name,
        TSignal signal,
        string? idempotencyKey = null,
        CancellationToken cancellationToken = default
    ) =>
        _runner.SignalAsync(workflowRunId, name, signal, idempotencyKey, cancellationToken);

    private static void EnsureType<TActual>(Type expected, string role, Type workflowType)
    {
        if (expected == typeof(TActual))
        {
            return;
        }

        throw new InvalidOperationException(
            $"Workflow '{workflowType.FullName}' {role} type is '{expected.FullName}', but the client call used '{typeof(TActual).FullName}'."
        );
    }
}
