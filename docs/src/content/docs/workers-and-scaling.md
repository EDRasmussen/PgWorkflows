---
title: Workers & scaling
description: Every process is a worker unless you say otherwise. Scale by deploying more of them.
---

Every process that calls `AddPgWorkflows` is a worker. Call `DisableWorkers()` and it's
a client. That's the whole model.

- A **worker** leases and executes workflows and activities.
- A **client** starts, signals, and awaits workflows, and never executes anything.

Postgres is the only coordination layer. There is no scheduler service, leader
election, or message broker.

:::note[Coming from Temporal?]
The scaling model is the same: run as many workers as you like. There is just no
server cluster to operate.
:::

## Workers

There is no worker setup. `AddPgWorkflows` registers a hosted background worker, so the
app that defines your workflows processes them. An ASP.NET API, a console app, and a
Windows service are all equally valid workers.

```csharp
builder.Services.AddPgWorkflows(pg =>
    pg.UsePostgres(connectionString)
        .AddWorkflow<TrialOnboardingWorkflow>()
        .AddActivities<EmailActivities>()
);
```

To scale, deploy more instances. Leases in Postgres make this safe:

- Two workers never run the same step (`FOR UPDATE SKIP LOCKED`).
- Leases are heartbeated while work runs, so slow work isn't stolen.
- A dead worker's lease expires; a peer resumes the run from the last completed step.
- A worker that lost its lease has its writes rejected, so a stale worker can't
  corrupt state.

## Clients

A front-facing API shouldn't compete for work. It should dispatch and move on.

```csharp
// API: pure client, runs no workers
builder.Services.AddPgWorkflows(pg =>
    pg.UsePostgres(connectionString)
        .DisableWorkers()
        .AddWorkflow<TrialOnboardingWorkflow>()
);
```

Starting a workflow is a single `INSERT`, cheap enough for your hottest request path.
Whichever worker leases the run first executes it.

For high-throughput APIs, prefer fire-and-forget: `StartAsync`, return the run id, let
callers check back. Awaiting `GetResultAsync` per request polls the database, which is
fine for tens of waiters but not a million.

## Sharing definitions

Put workflows and activities in a shared class library; every participating process
registers from it:

```text
MyApp.Workflows/   ← workflow + activity classes
MyApp.Api/         ← client: AddWorkflow + DisableWorkers
MyApp.Worker/      ← worker: AddWorkflow + AddActivities
```

Clients need `AddWorkflow` (to resolve names and types) but can skip `AddActivities`,
since activities only matter where they execute.

## Tuning

All knobs and defaults are in the [configuration reference](/reference/configuration/).
Two worth knowing early:

- `WorkerId` defaults to the machine name; set it explicitly in containers so leases
  are attributable when debugging.
- Activity `MaxConcurrency` defaults to four per processor (IO-friendly); lower it for
  CPU-bound work.
- Each process holds its own connection pool, capped at 20 by default. The sum across
  all API and worker processes must stay below Postgres' `max_connections`; see
  [connection pooling](/reference/configuration/#connection-pooling).
