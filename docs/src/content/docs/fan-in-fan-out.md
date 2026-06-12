---
title: Fan-in fan-out
description: Run activities in parallel with ctx.WhenAll and gather their results durably.
---

`ctx.WhenAll` runs several activities concurrently and resumes the workflow once all of
them have completed. Like every step, the fan-out is durable: a crash mid-flight
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

The whole batch is enqueued together, so activity workers pick the branches up side by
side; how many actually run at once is bounded by your workers' `MaxConcurrency`.

## What happens on failure?

`ctx.WhenAll` matches `Task.WhenAll` semantics: it waits for **every** branch to finish
before surfacing anything, then throws the first failure (by position). Branches that
succeeded keep their recorded results; they are memoized like any other step and won't
re-run. The failure then propagates like any workflow failure: the run fails (after its
[workflow-level attempts](/error-handling/)) and any registered
[compensations](/error-handling/#compensation-with-ctxonfailure) run.

## How it stays durable

Each branch is its own persisted step backed by its own activity job, so a fan-out of
fifty is fifty rows, each completing and memoizing independently. While branches
execute, the run parks without holding a worker slot, and the completion of the last
outstanding branch wakes it. A crash mid-fan-out resumes exactly: finished branches
replay from their stored results, and unfinished ones are simply waited for again.
