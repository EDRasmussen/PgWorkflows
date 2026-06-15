---
title: Signals
description: Deliver external events into a running workflow, such as approvals, webhooks, and user actions.
---

Signals let the outside world talk to a running workflow: a human approval, a webhook,
a payment confirmation. The workflow parks until the signal arrives, and signals are
persisted so nothing is lost across restarts.

## Waiting for a signal

```csharp
[Workflow("approval")]
public sealed class ApprovalWorkflow
{
    [WorkflowRun]
    public async ValueTask<string> RunAsync(
        IWorkflowContext ctx,
        string request,
        CancellationToken cancellationToken
    )
    {
        var decision = await ctx.WaitForSignal<ApprovalDecision>("approval", cancellationToken);
        return decision.Approved ? $"{request} approved." : $"{request} rejected.";
    }
}
```

## Sending a signal

Send through the handle you got when starting the workflow, or through the client with
a run id:

```csharp
await handle.SignalAsync(
    "approval",
    new ApprovalDecision(Approved: true, UserId: "era", Comment: "Ship it"),
    idempotencyKey: $"approval:{handle.WorkflowRunId:N}"
);
```

More commonly the signal comes from a different process, like an HTTP endpoint handling
the approval click. Any process with the run id can signal through the client:

```csharp
app.MapPost(
    "/approvals/{runId:guid}",
    async (Guid runId, ApprovalDecision decision, IPgWorkflowClient workflows) =>
    {
        await workflows.SignalAsync(runId, "approval", decision);
        return Results.Accepted();
    }
);
```

## Delivery semantics

Every signal is a persisted row in Postgres:

- **Buffered.** Delivering before the workflow reaches its `WaitForSignal` is fine; the
  payload waits in Postgres until the workflow asks for it.
- **FIFO per name.** Multiple signals with the same name are consumed in delivery order,
  one per `WaitForSignal` call.
- **Idempotent with a key.** Pass an `idempotencyKey` so a retried sender doesn't
  deliver the same signal twice; a redelivery with a known key buffers nothing.
- **Terminal runs reject signals.** Signalling a run that already succeeded or failed
  throws, so a lost decision surfaces at the sender.

Like `ctx.Sleep`, waiting parks the run by throwing an internal control-flow exception,
so don't wrap `ctx.WaitForSignal` in a broad `catch` that would swallow the park.
