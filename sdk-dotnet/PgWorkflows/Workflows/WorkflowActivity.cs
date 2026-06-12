namespace PgWorkflows.Workflows;

/// <summary>
/// A pending activity created by <see cref="IWorkflowContext.CallActivity{TActivities, TOutput}(System.Linq.Expressions.Expression{Func{TActivities, TOutput}})"/>,
/// to be awaited as part of a fan-out via <c>ctx.WhenAll(...)</c>.
/// </summary>
public readonly struct WorkflowActivity<TOutput>
{
    internal WorkflowActivity(WorkflowActivityCall call) => Call = call;

    internal WorkflowActivityCall? Call { get; }
}
