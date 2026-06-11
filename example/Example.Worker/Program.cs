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
    // MaxConcurrency defaults to 10 per worker; the pool sizes itself to fit, so there is nothing
    // else to tune here.
    pg.UsePostgres(connectionString)
        .ConfigureWorkflowWorker(options => options with { WorkerId = $"{workerId}-workflows" })
        .ConfigureActivityWorker(options => options with { WorkerId = $"{workerId}-activities" })
        .AddWorkflow<GreetingWorkflow>()
        .AddActivities<GreetingActivities>()
);

var host = builder.Build();

Console.WriteLine($"[{workerId}] Waiting for work...");
host.Run();
