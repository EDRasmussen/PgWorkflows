using Example.Workflows;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using PgWorkflows;

var connectionString =
    Environment.GetEnvironmentVariable("PGWORKFLOWS_CONNECTION_STRING")
    ?? "Host=localhost;Port=55432;Database=pgworkflows;Username=postgres;Password=postgres";

var workerId =
    Environment.GetEnvironmentVariable("PGWORKFLOWS_WORKER_ID") ?? Environment.MachineName;

var builder = Host.CreateApplicationBuilder(args);

// A worker: leases and executes workflows and activities until stopped.
builder.Services.AddPgWorkflows(pg =>
    pg.UsePostgres(connectionString)
        .ConfigureWorkflowWorker(options =>
            options with
            {
                WorkerId = $"{workerId}-workflows",
                // Greeting workflows are pure orchestration (no compute between awaits), so a
                // pass is all database IO and the CPU-count default leaves the worker mostly
                // idle. Both knobs must rise together: the worker leases
                // min(BatchSize, MaxConcurrency) runs per pass.
                BatchSize = 64,
                MaxConcurrency = 64,
            }
        )
        .ConfigureActivityWorker(options =>
            options with
            {
                WorkerId = $"{workerId}-activities",
                // Same reasoning as the workflow worker: these activities are trivial IO, and
                // the default min(BatchSize 16, MaxConcurrency) leases only 16 jobs per pass.
                BatchSize = 64,
                MaxConcurrency = 64,
            }
        )
        .AddWorkflow<GreetingWorkflow>()
        .AddActivities<GreetingActivities>()
);

var host = builder.Build();

Console.WriteLine($"[{workerId}] Waiting for work...");
host.Run();
