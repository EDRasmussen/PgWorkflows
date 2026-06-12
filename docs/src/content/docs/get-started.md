---
title: Get started
description: Install PgWorkflows, define your first workflow, and run it against your existing Postgres.
---

Get a durable workflow running in about five minutes, using your app and a Postgres
connection string.

## Prerequisites

- .NET 8.0 or later
- A reachable Postgres instance (any flavor: local, Docker, RDS, Supabase, Neon)

## Install

```sh
dotnet add package PgWorkflows
```

## Register PgWorkflows

Point the builder at your Postgres connection string and register your workflows and
activities. The hosted worker is configured for you.

```csharp
builder.Services.AddPgWorkflows(pg =>
    pg.UsePostgres(connectionString)
        .AddWorkflow<GreetingWorkflow>()
        .AddActivities<HelloActivities>()
);
```

The tables PgWorkflows needs are created automatically on startup (idempotently, safe
across many instances starting at once). Pass `ensureSchemaOnStart: false` to
`UsePostgres` if your deployment applies schema out-of-band instead.

## Define a workflow and its activities

A workflow is an ordinary C# class; activities hold the side effects.

```csharp
[Workflow("greeting")]
public sealed class GreetingWorkflow
{
    [WorkflowRun]
    public async ValueTask<string> RunAsync(
        IWorkflowContext ctx,
        string name,
        CancellationToken cancellationToken
    )
    {
        return await ctx.Activity(
            (HelloActivities a) => a.Hello(name),
            cancellationToken
        );
    }
}

public sealed class HelloActivities
{
    [Activity("hello")]
    public string Hello(string name) => $"Hello, {name}!";
}
```

## Start it

```csharp
var workflows = app.Services.GetRequiredService<IPgWorkflowClient>();
var result = await workflows.ExecuteAsync<GreetingWorkflow, string, string>("Postgres");
```

`result` is `"Hello, Postgres!"`. The interesting part is what's now in your database.
The run, its durable step, and the activity job are all plain rows:

```sql
select workflow_name, status, result from pw_workflow_runs;

 workflow_name | status    | result
---------------+-----------+---------------------
 greeting      | succeeded | "Hello, Postgres!"
```

Everything PgWorkflows knows lives in tables like this one; see
[what's in your database](/how-it-works/#whats-in-your-database) for the tour.

## Next steps

- Understand the mental model in [how PgWorkflows works](/how-it-works/).
- Explore [fan-in fan-out](/fan-in-fan-out/), [sleep](/sleep/), and
  [signals](/signals/).
