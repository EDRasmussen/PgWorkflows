using Npgsql;
using PgWorkflows.Activities;
using PgWorkflows.Jobs;
using PgWorkflows.Persistence.Postgres;
using PgWorkflows.Workers;

var connectionString = Environment.GetEnvironmentVariable("PGWORKFLOWS_CONNECTION_STRING");
if (string.IsNullOrWhiteSpace(connectionString))
{
    throw new InvalidOperationException(
        "Set the PGWORKFLOWS_CONNECTION_STRING environment variable before running the sample."
    );
}

await using var dataSource = NpgsqlDataSource.Create(connectionString);
var store = new PostgresActivityJobStore(dataSource);
await store.EnsureSchemaAsync();

var registry = new ActivityRegistry();
registry.Register(
    "hello",
    static (context, input, _) =>
    {
        var name = string.IsNullOrWhiteSpace(input) ? "world" : input;
        return ValueTask.FromResult<string?>(
            $"Hello, {name}. Job {context.JobId} ran on attempt {context.Attempt}."
        );
    }
);

var jobId = await store.EnqueueAsync(
    new EnqueueActivityRequest(ActivityName: "hello", Input: "Postgres")
);

var worker = new ActivityWorker(
    registry,
    store,
    new ActivityWorkerOptions { WorkerId = "console-sample", BatchSize = 1 }
);

var processed = await worker.RunOnceAsync();
var job =
    await store.GetAsync(jobId)
    ?? throw new InvalidOperationException($"Job '{jobId}' was not found after execution.");

Console.WriteLine($"Processed jobs: {processed}");
Console.WriteLine($"Job id: {job.JobId}");
Console.WriteLine($"Status: {job.Status}");
Console.WriteLine($"Result: {job.Result ?? "<null>"}");
