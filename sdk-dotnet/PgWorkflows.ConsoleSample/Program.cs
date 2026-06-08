using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using PgWorkflows;
using PgWorkflows.Activities;
using PgWorkflows.Workflows;

var connectionString = Environment.GetEnvironmentVariable("PGWORKFLOWS_CONNECTION_STRING");
if (string.IsNullOrWhiteSpace(connectionString))
{
    throw new InvalidOperationException(
        "Set the PGWORKFLOWS_CONNECTION_STRING environment variable before running the sample."
    );
}

var builder = Host.CreateApplicationBuilder(args);
builder.Services.AddPgWorkflows(pg =>
    pg.UsePostgres(connectionString)
        .ConfigureWorkflowWorker(options =>
            options with
            {
                WorkerId = "console-sample-workflows",
                BatchSize = 1,
                PollInterval = TimeSpan.FromMilliseconds(100),
            }
        )
        .AddWorkflow<GreetingWorkflow>()
        .AddActivities<HelloActivities>()
);

using var app = builder.Build();
await app.StartAsync();

try
{
    var workflows = app.Services.GetRequiredService<IPgWorkflowClient>();
    var handle = await workflows.StartAsync<GreetingWorkflow, GreetingWorkflowInput, string>(
        new GreetingWorkflowInput("Postgres", 42),
        idempotencyKey: "console-sample"
    );
    var result = await handle.GetResultAsync();

    Console.WriteLine($"Workflow run id: {handle.WorkflowRunId}");
    Console.WriteLine($"Workflow result: {result}");
}
finally
{
    await app.StopAsync();
}

internal sealed record GreetingWorkflowInput(string Name, int GoodbyeId);

[Workflow("console-sample-workflow")]
internal sealed class GreetingWorkflow
{
    [WorkflowRun]
    public async ValueTask<string> RunAsync(
        IWorkflowContext ctx,
        GreetingWorkflowInput input,
        CancellationToken cancellationToken
    )
    {
        var hello = await ctx.Activity(
            (HelloActivities activities) => activities.Hello(input.Name),
            cancellationToken
        );
        var goodbye = await ctx.Activity(
            (HelloActivities activities) => activities.Goodbye(input.GoodbyeId),
            cancellationToken
        );

        return $"{hello} {goodbye}";
    }
}

internal sealed class HelloActivities
{
    [Activity("hello")]
    public string Hello(string name) =>
        $"Hello, {(string.IsNullOrWhiteSpace(name) ? "world" : name)}.";

    [Activity("goodbye")]
    public string Goodbye(int id) => $"Goodbye, {id}.";
}
