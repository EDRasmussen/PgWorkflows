namespace PgWorkflows.Workflows;

public readonly struct WorkflowActivity<TOutput>
{
    internal WorkflowActivity(WorkflowActivityCall call) => Call = call;

    internal WorkflowActivityCall? Call { get; }
}
