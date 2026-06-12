---
title: Error handling & compensation
description: Retries, failure policies, and saga-style compensation with ctx.OnFailure.
---

Activities fail: networks blip, label printers go offline. PgWorkflows gives a workflow
a retry budget, keeps infrastructure errors from counting against it, and when a run
fails permanently partway through, `ctx.OnFailure` runs the compensations you
registered, in reverse, so completed side effects get undone.

## Workflow retries

`MaxAttempts` on the workflow worker options is the whole-workflow retry budget
(default 1), with backoff from `GetRetryDelay`:

```csharp
pg.ConfigureWorkflowWorker(options => options with { MaxAttempts = 3 })
```

A retry replays the workflow method: completed steps return their memoized results, so
the retry resumes from where the failure happened rather than redoing side effects.
This makes the budget effective against exceptions thrown by your workflow code.

An exception thrown *inside an activity* is different: it fails that step durably, and
the step's recorded failure replays on every subsequent attempt. Today each activity
step gets a single execution attempt. Retry the side effect inside the activity itself
if it is transient, or let the failure fail the workflow and compensate. Configurable
per-activity retry budgets are planned.

## Compensation with `ctx.OnFailure`

Register a compensating activity right after the step it undoes. If a later step fails
permanently, the registered compensations run.

```csharp
var reservation = await ctx.Activity(
    (CheckoutActivities a) => a.ReserveInventory(input.UserName, itemName),
    cancellationToken
);

await ctx.OnFailure(
    (CheckoutActivities a) => a.ReleaseInventory(reservation.ReservationId),
    cancellationToken
);

var payment = await ctx.Activity(
    (CheckoutActivities a) => a.ChargePayment(input.UserName, input.Amount),
    cancellationToken
);

await ctx.OnFailure(
    (CheckoutActivities a) => a.RefundPayment(payment.PaymentId),
    cancellationToken
);

// If this throws permanently, the refund and the release both run.
await ctx.Activity(
    (CheckoutActivities a) => a.CreateShipment(input.UserName, itemName),
    cancellationToken
);
```

Compensations run after the run's last attempt fails, in reverse registration order:
the refund before the release, unwinding the saga the way it was built. Each hook is
itself a durable step: its arguments are captured and persisted at registration time,
it executes exactly once, and a hook's own failure is recorded alongside the run's
error.

## Infrastructure errors

A transient database error (connection exhaustion, a dropped connection, a serialization
failure) says nothing about your workflow. When one interrupts a run mid-execution, the
worker does not record a failure. The run is released back to pending, the attempt the
lease charged is rolled back, and the worker backs off before leasing more work. The run
is retried after the normal retry delay with its attempt budget intact, so a workflow
with `MaxAttempts = 1` still survives a database hiccup.

Attempts are only consumed when the workflow itself throws.

## What the caller sees

A caller awaiting `GetResultAsync` (or `ExecuteAsync`) on a run that fails terminally
gets an `InvalidOperationException` carrying the recorded error. The failure also lives
in Postgres (`pw_workflow_runs.status = 'failed'`, with the full error text in the
`error` column), so a fire-and-forget caller can find it later with plain SQL.
