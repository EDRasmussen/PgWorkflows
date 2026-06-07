namespace PgWorkflows.Workflows;

internal sealed record WorkflowDefinition(
    Type WorkflowType,
    string Name,
    Type InputType,
    Type OutputType,
    Func<IServiceProvider, IWorkflowContext, object?, CancellationToken, ValueTask<object?>> InvokeAsync
);
