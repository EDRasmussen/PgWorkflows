using Example.Workflows;
using PgWorkflows;
using PgWorkflows.Workflows;
using Scalar.AspNetCore;

var connectionString =
    Environment.GetEnvironmentVariable("PGWORKFLOWS_CONNECTION_STRING")
    ?? "Host=localhost;Port=55432;Database=pgworkflows;Username=postgres;Password=postgres";

var builder = WebApplication.CreateBuilder(args);

builder.Services.AddOpenApi();

// Pure client: starts workflows but never executes them; the worker fleet does.
builder.Services.AddPgWorkflows(pg =>
    pg.UsePostgres(connectionString).DisableWorkers().AddWorkflow<GreetingWorkflow>()
);

var app = builder.Build();

app.MapOpenApi();
app.MapScalarApiReference();

app.MapPost(
        "/greetings",
        async (
            GreetingInput input,
            IPgWorkflowClient workflows,
            CancellationToken cancellationToken
        ) =>
        {
            var handle = await workflows.StartAsync<GreetingWorkflow, GreetingInput, string>(
                input,
                cancellationToken: cancellationToken
            );

            return Results.Accepted(value: new { workflowRunId = handle.WorkflowRunId });
        }
    )
    .WithSummary("Queue a greeting workflow")
    .WithDescription(
        "Inserts a workflow run and returns immediately; a worker process picks it up and executes it."
    )
    .Produces(StatusCodes.Status202Accepted);

app.Run();
