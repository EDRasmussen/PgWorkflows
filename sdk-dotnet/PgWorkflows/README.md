# PgWorkflows

Durable workflows on Postgres. Plain async C# methods become crash-proof, exactly-once
orchestrations — no separate server, no replay rules to learn. Your app and a handful of
Postgres tables do all the work.

```csharp
[Workflow("trial-onboarding")]
public sealed class TrialOnboardingWorkflow
{
    [WorkflowRun]
    public async ValueTask<string> RunAsync(
        IWorkflowContext ctx, SignupInput input, CancellationToken ct)
    {
        await ctx.Activity((EmailActivities a) => a.SendWelcome(input.Email), ct);

        await ctx.Sleep(TimeSpan.FromDays(11), ct);   // survives restarts and deploys

        var decision = await ctx.WaitForSignal<UpgradeDecision>("upgrade", ct);
        return decision.Plan;
    }
}
```

Every `ctx.*` call is a durable step: results are persisted, and if the process dies
another worker resumes the run — already-completed steps return their stored results
instead of re-running, so side effects happen exactly once.

## Getting started

```csharp
builder.Services.AddPgWorkflows(pg =>
    pg.UsePostgres(connectionString)
        .AddWorkflow<TrialOnboardingWorkflow>()
        .AddActivities<EmailActivities>()
);
```

That's a complete worker: it creates the schema, then leases and executes workflows and
activities in the background. Start a run from anywhere:

```csharp
var handle = await workflows.StartAsync<TrialOnboardingWorkflow, SignupInput, string>(
    input, idempotencyKey: $"signup:{signupId}");
```

## What you get

- **Durable steps** — activity results are memoized per run; crashes and deploys resume, never re-run.
- **Fan-out / fan-in** — `ctx.WhenAll(...)` dispatches activities concurrently and parks the run until all land.
- **Durable timers** — `ctx.Sleep(...)` parks the run in the database; sleeping costs no thread or worker slot.
- **Signals** — `ctx.WaitForSignal<T>(...)` with buffering, FIFO consumption, and idempotent delivery.
- **Failure hooks** — saga-style compensation via `ctx.OnFailure(...)` after terminal failure.
- **Scale out by running more instances** — workers coordinate through `FOR UPDATE SKIP LOCKED` leases with heartbeats and crash reclaim.
- **Observable** — structured logging, OpenTelemetry spans, and plain SQL-inspectable state.

## Learn more

Documentation, examples, and source: [github.com/EDRasmussen/PgWorkflows](https://github.com/EDRasmussen/PgWorkflows)
