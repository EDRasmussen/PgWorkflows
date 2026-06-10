---
title: Sleep
description: Durable timers that pause a workflow for minutes or months without holding a worker.
---

`ctx.Sleep` pauses a workflow for any duration, from seconds to weeks. The run is parked in
Postgres and resumed by a worker when the timer fires, so it survives restarts, deploys,
and crashes. A sleeping workflow does not hold a thread, a worker slot, or any memory.

## Usage

```csharp
await ctx.Activity((EmailActivities a) => a.SendWelcome(input.Email), cancellationToken);

await ctx.Sleep(TimeSpan.FromDays(11), cancellationToken);

await ctx.Activity((EmailActivities a) => a.SendTrialEndingReminder(input.Email), cancellationToken);
```

## How it works

<!-- TODO: the run's visible_at is pushed into the future and its lease released; the
     workflow worker picks it back up when the timer fires. The deadline is persisted on
     first encounter, so it stays stable across replays; resuming doesn't restart the
     clock. -->

## Caveats

<!-- TODO, from the API docs:
     - Parking is implemented via an internal control-flow exception, so don't wrap
       ctx.Sleep in a broad catch, or the park is swallowed (the run then fails loudly
       rather than silently skipping the timer).
     - Sleeping requires the hosted workflow worker (the AddPgWorkflows default); it
       throws NotSupportedException on an inline client (executeWorkflowsInCaller: true). -->
