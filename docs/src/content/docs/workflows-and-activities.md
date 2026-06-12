---
title: Workflows & activities
description: Define workflows as plain C# classes and put side effects in activities.
---

Workflows orchestrate; activities do. The workflow method describes the durable control
flow, while every side effect (HTTP calls, emails, database writes) lives in an
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

`[Workflow("greeting")]` sets the workflow's durable identity: the name stored with
every run in Postgres. It defaults to the class name, but pin it explicitly for anything
long-lived, because renaming a class without a pinned name orphans its in-flight runs.

`[WorkflowRun]` marks the single public entry-point method. Its signature is
`(IWorkflowContext, input, CancellationToken)`: exactly one context, exactly one input
parameter, and an optional trailing cancellation token. The method can return `void`,
`Task`, `ValueTask`, or their generic forms.

Inputs and outputs are stored as JSON in Postgres, so any JSON-serializable type works.
Records are a great fit:

```csharp
public sealed record SignupInput(string Company, string Email);
```

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

An activity class is a normal class resolved from your DI container, so constructor
inject whatever the side effect needs, like an `HttpClient` or a `DbContext`. Methods
can be sync, `Task`, or `ValueTask`, take multiple parameters, and accept an optional
trailing `CancellationToken` that is cancelled if the activity loses its lease. Like
workflows, `[Activity]` names default to the method name; pin them for anything
long-lived.

## Calling activities from a workflow

```csharp
var receipt = await ctx.Activity(
    (PaymentActivities a) => a.Charge(input.UserName, input.Amount),
    cancellationToken
);
```

The lambda must be a direct method call on the activity class; that expression is how
PgWorkflows knows which activity to enqueue and with which arguments. The arguments are
evaluated and serialized at the call site, the activity runs on whichever worker leases
the job, and the workflow parks until the result lands.

`ctx.Activity` awaits one activity. To run several concurrently, create pending
activities with `ctx.CallActivity` and await them together with `ctx.WhenAll`; see
[fan-in fan-out](/fan-in-fan-out/).

## Registration

```csharp
builder.Services.AddPgWorkflows(pg =>
    pg.UsePostgres(connectionString)
        .AddWorkflow<GreetingWorkflow>()
        .AddActivities<EmailActivities>()
);
```

`AddWorkflow<T>()` registers one workflow class; `AddActivities<T>()` registers every
`[Activity]` method on a class. Workflow and activity names must be unique across the
registration, and duplicates fail at startup.
