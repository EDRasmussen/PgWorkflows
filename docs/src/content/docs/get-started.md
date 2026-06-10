---
title: Get started
description: Install PgWorkflows, define your first workflow, and run it against your existing Postgres.
---

Get a durable workflow running in about five minutes, using your app and a Postgres
connection string.

## Prerequisites

<!-- TODO: .NET version, a reachable Postgres instance (any flavor: local, RDS, Supabase, ...). -->

## Install

<!-- TODO: NuGet install command once the package is published. -->

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

<!-- TODO: mention ensureSchemaOnStart: the schema is created automatically on startup by default. -->

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

<!-- TODO: expected console output, and a pointer to the tables created in Postgres so readers can peek at the state. -->

## Next steps

- Understand the mental model in [how PgWorkflows works](/how-it-works/).
- Explore [fan-in fan-out](/fan-in-fan-out/), [sleep](/sleep/), and
  [signals](/signals/).
