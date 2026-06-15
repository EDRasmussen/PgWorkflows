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

When the workflow reaches `ctx.Sleep`, the run's `visible_at` is pushed to the deadline
and its lease is released, freeing the worker for other work immediately. When the
deadline passes, the run becomes visible again and the next free worker resumes it,
replaying the already-completed steps from their stored results.

The deadline is persisted the first time the sleep is encountered, so it stays stable
across replays: a run that crashes and resumes mid-sleep does not restart the clock, and
a sleep that already elapsed is skipped on replay instead of sleeping again.

## Caveats

Parking is implemented by throwing an internal control-flow exception that unwinds the
workflow method, so don't wrap `ctx.Sleep` in a broad `catch`, which would swallow the
park. If it happens, the run fails loudly rather than silently skipping the timer.

Timer precision is bounded by worker polling: the run resumes on the first poll after
the deadline, so the resolution is roughly the worker's `PollInterval` (250 ms by
default). For sleeps measured in minutes to months this is irrelevant.
