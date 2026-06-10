---
title: Error handling & compensation
description: Retries, failure policies, and saga-style compensation with ctx.OnFailure.
---

Activities fail: networks blip, label printers go offline. PgWorkflows retries
activities for you, and when a workflow fails permanently partway through, `ctx.OnFailure`
runs the compensations you registered, in reverse, so completed side effects get undone.

## Activity retries

<!-- TODO: attempts and backoff: MaxAttempts and GetRetryDelay on the worker options;
     what counts as a retryable failure; what happens when retries are exhausted. -->

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

<!-- TODO: full runnable version lives in PgWorkflows.ConsoleSample (CheckoutWorkflow);
     state the execution order guarantee (reverse registration order?) and that
     registered compensations are themselves durable steps. -->

## Workflow failure

<!-- TODO: what the caller sees: GetResultAsync / ExecuteAsync throwing with the
     failure; where the failure is recorded in Postgres. -->
