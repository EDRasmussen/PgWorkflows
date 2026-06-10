using PgWorkflows.Workflows;

namespace Example.Workflows;

public sealed record GreetingInput(string Name);

[Workflow("example-greeting")]
public sealed class GreetingWorkflow
{
    [WorkflowRun]
    public async ValueTask<string> RunAsync(
        IWorkflowContext ctx,
        GreetingInput input,
        CancellationToken cancellationToken
    )
    {
        var greeting = await ctx.Activity(
            (GreetingActivities activities) => activities.ComposeGreeting(input.Name),
            cancellationToken
        );

        return await ctx.Activity(
            (GreetingActivities activities) => activities.DeliverGreeting(greeting),
            cancellationToken
        );
    }
}
