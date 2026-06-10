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

<!-- TODO: also show IPgWorkflowClient.SignalAsync(workflowRunId, name, payload) for the
     common case where the signal comes from a different process (e.g. an HTTP endpoint). -->

## Delivery semantics

<!-- TODO, from the API docs:
     - Signals are buffered: delivering before the workflow reaches WaitForSignal is fine.
     - Multiple signals with the same name are consumed in delivery order.
     - Use an idempotency key so a retried sender doesn't deliver the signal twice.
     - Same parking caveat as Sleep: requires the hosted worker; don't wrap the wait in a
       broad catch. -->
