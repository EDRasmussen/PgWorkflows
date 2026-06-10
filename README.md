# PgWorkflows

Durable workflows built on PostgreSQL.

PgWorkflows lets you build long-running, scalable, durable workflows on top of the Postgres
database you already run. You don't need to host a separate workflow server or adopt a vendor
ecosystem; a connection string is enough.

📖 **Docs:** [pgworkflows.emildr.dk](https://pgworkflows.emildr.dk)

## Features

- **Durable execution**: workflow state lives in Postgres and survives restarts and deploys
- **Fan-in / fan-out**: run activities in parallel and join the results
- **Durable timers**: sleep for days without a worker holding anything in memory
- **Signals**: park a workflow until an external event arrives, like a human decision
- **Scales with your workers**: add more workers when you need more throughput, without a
  coordinator service

## Quick start

Install the package:

```sh
dotnet add package PgWorkflows
```

Register PgWorkflows with your Postgres connection string:

```csharp
builder.Services.AddPgWorkflows(pg =>
    pg.UsePostgres(connectionString)
        .AddWorkflow<GreetingWorkflow>()
        .AddActivities<HelloActivities>()
);
```

Define a workflow. A workflow is an ordinary C# class, and activities hold the side effects:

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

Start it:

```csharp
var workflows = app.Services.GetRequiredService<IPgWorkflowClient>();
var result = await workflows.ExecuteAsync<GreetingWorkflow, string, string>("Postgres");
```

See the [get started guide](https://pgworkflows.emildr.dk/get-started/) for the full walkthrough.

## A real-world example

Fan-out, durable sleep, and human-in-the-loop signals in one workflow:

```csharp
[Workflow("trial-onboarding")]
public sealed class TrialOnboardingWorkflow
{
    [WorkflowRun]
    public async ValueTask<string> RunAsync(
        IWorkflowContext ctx,
        SignupInput input,
        CancellationToken cancellationToken
    )
    {
        // Fan-out: run independent activities in parallel.
        var (workspace, _) = await ctx.WhenAll(
            ctx.CallActivity((OnboardingActivities a) => a.ProvisionWorkspace(input.Company)),
            ctx.CallActivity((EmailActivities a) => a.SendWelcome(input.Email)),
            cancellationToken
        );

        // Durable timer: the run is parked in Postgres. It survives
        // restarts and deploys, and no worker holds it in memory.
        await ctx.Sleep(TimeSpan.FromDays(11), cancellationToken);

        await ctx.Activity(
            (EmailActivities a) => a.SendTrialEndingReminder(input.Email),
            cancellationToken
        );

        // Human-in-the-loop: park again until an external signal arrives.
        var decision = await ctx.WaitForSignal<UpgradeDecision>("upgrade", cancellationToken);

        if (!decision.Upgraded)
        {
            await ctx.Activity(
                (OnboardingActivities a) => a.DowngradeToFreeTier(workspace.Id),
                cancellationToken
            );
            return $"{input.Company} stayed on the free tier.";
        }

        await ctx.Activity(
            (BillingActivities a) => a.StartSubscription(workspace.Id, decision.Plan),
            cancellationToken
        );
        return $"{input.Company} upgraded to {decision.Plan}.";
    }
}
```

## Learn more

- [Get started](https://pgworkflows.emildr.dk/get-started/) walks you through your first workflow
  in about five minutes
- [How it works](https://pgworkflows.emildr.dk/how-it-works/) explains the mental model under the
  hood
- [Fan-in fan-out](https://pgworkflows.emildr.dk/fan-in-fan-out/),
  [Sleep](https://pgworkflows.emildr.dk/sleep/), [Signals](https://pgworkflows.emildr.dk/signals/),
  [Error handling](https://pgworkflows.emildr.dk/error-handling/)

## License

PgWorkflows is licensed under the [MIT License](LICENSE).
