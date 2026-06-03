using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using PgWorkflows;
using PgWorkflows.Activities;
using PgWorkflows.Jobs;
using PgWorkflows.Persistence;

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
        .ConfigureWorker(options =>
            options with
            {
                WorkerId = "console-sample",
                BatchSize = 1,
                PollInterval = TimeSpan.FromMilliseconds(100),
            }
        )
        .AddActivities<HelloActivities>()
);

using var app = builder.Build();
await app.StartAsync();

var store = app.Services.GetRequiredService<IActivityJobStore>();
var helloId = await store.EnqueueAsync("hello", "Postgres");
var goodbyeId = await store.EnqueueAsync("goodbye", 42);
var hello = await WaitForTerminalAsync(store, helloId, TimeSpan.FromSeconds(10));
var goodbye = await WaitForTerminalAsync(store, goodbyeId, TimeSpan.FromSeconds(10));

Console.WriteLine($"Hello job id: {hello.JobId}");
Console.WriteLine($"Hello status: {hello.Status}");
Console.WriteLine($"Hello result: {hello.GetResult<string>() ?? "<null>"}");
Console.WriteLine($"Goodbye job id: {goodbye.JobId}");
Console.WriteLine($"Goodbye status: {goodbye.Status}");
Console.WriteLine($"Goodbye result: {goodbye.GetResult<string>() ?? "<null>"}");

await app.StopAsync();

static async Task<ActivityJob> WaitForTerminalAsync(
    IActivityJobStore store,
    Guid jobId,
    TimeSpan timeout
)
{
    using var deadline = new CancellationTokenSource(timeout);
    while (!deadline.IsCancellationRequested)
    {
        var job = await store.GetAsync(jobId, deadline.Token);
        if (job is { Status: JobStatus.Succeeded or JobStatus.Failed })
        {
            return job;
        }

        await Task.Delay(100, deadline.Token);
    }

    throw new TimeoutException($"Job {jobId} did not reach a terminal state in {timeout}.");
}

internal sealed class HelloActivities
{
    [Activity("hello")]
    public string Hello(string name) =>
        $"Hello, {(string.IsNullOrWhiteSpace(name) ? "world" : name)}.";

    [Activity("goodbye")]
    public string Goodbye(int id) => $"Goodbye, {id}.";
}
