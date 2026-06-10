---
title: Fan-in fan-out
description: Run activities in parallel with ctx.WhenAll and gather their results durably.
---

`ctx.WhenAll` runs several activities concurrently and resumes the workflow once all of
them have completed — and like every step, the fan-out is durable: a crash mid-flight
resumes cleanly without re-running finished branches.

## Parallel activities with typed results

Use `ctx.CallActivity` to create the pending activities, then await them together. Up to
three differently-typed activities come back as a tuple:

```csharp
var (hello, goodbye) = await ctx.WhenAll(
    ctx.CallActivity((HelloActivities a) => a.Hello(input.Name)),
    ctx.CallActivity((HelloActivities a) => a.Goodbye(input.GoodbyeId)),
    cancellationToken
);
```

## Fan-out over a collection

When every branch has the same result type, pass a sequence and get an array back:

```csharp
var results = await ctx.WhenAll(
    customers.Select(c =>
        ctx.CallActivity((EmailActivities a) => a.SendNewsletter(c.Email))
    ),
    cancellationToken
);
```

<!-- TODO: verify/polish the collection example; show realistic input. -->

## What happens on failure?

<!-- TODO: semantics when one branch fails — what happens to the others, how retries
     interact with WhenAll. -->

## How it stays durable

<!-- TODO: each branch is its own persisted step; the run parks while branches execute
     and wakes when the last one completes. -->
