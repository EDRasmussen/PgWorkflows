---
title: Workflows & activities
description: Define workflows as plain C# classes and put side effects in activities.
---

Workflows orchestrate; activities do. The workflow method describes the durable control
flow, while every side effect — HTTP calls, emails, database writes — lives in an
activity so it can be retried and recorded exactly once.

## Defining a workflow

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
        // ...
    }
}
```

<!-- TODO: explain [Workflow("name")] (the durable identity of the workflow type),
     [WorkflowRun], the (ctx, input, cancellationToken) signature, and supported
     input/output types (JSON-serializable records work great). -->

## Defining activities

```csharp
public sealed class EmailActivities
{
    [Activity("send-welcome")]
    public string SendWelcome(string email)
    {
        // side effects go here
    }
}
```

<!-- TODO: activity classes are resolved from DI; methods can be sync, Task, or
     ValueTask; multiple parameters are supported. -->

## Calling activities from a workflow

```csharp
var receipt = await ctx.Activity(
    (PaymentActivities a) => a.Charge(input.UserName, input.Amount),
    cancellationToken
);
```

<!-- TODO: ctx.Activity awaits one activity; ctx.CallActivity creates a pending
     WorkflowActivity for composition with ctx.WhenAll (see fan-in fan-out). Explain the
     lambda-expression style: it's how PgWorkflows knows which activity to enqueue and
     with which arguments. -->

## Registration

```csharp
builder.Services.AddPgWorkflows(pg =>
    pg.UsePostgres(connectionString)
        .AddWorkflow<GreetingWorkflow>()
        .AddActivities<EmailActivities>()
);
```

<!-- TODO: AddWorkflow per workflow type, AddActivities per activity class. -->
