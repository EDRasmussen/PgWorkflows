---
title: Configuration
description: PgWorkflowsBuilder methods and worker options reference.
---

Everything is configured through `AddPgWorkflows(pg => ...)` on your service collection.

## PgWorkflowsBuilder

```csharp
builder.Services.AddPgWorkflows(pg =>
    pg.UsePostgres(connectionString)
        .ConfigureWorkflowWorker(options => options with { /* ... */ })
        .ConfigureActivityWorker(options => options with { /* ... */ })
        .AddWorkflow<TrialOnboardingWorkflow>()
        .AddActivities<EmailActivities>()
);
```

| Method | Purpose |
| ------ | ------- |
| `UsePostgres(connectionString, ensureSchemaOnStart = true, maxPoolSize = null)` | Point PgWorkflows at your database. Creates the schema on startup by default and caps the connection pool at 20 per process unless told otherwise (see [connection pooling](#connection-pooling)). |
| `DisableWorkers()` | Client-only mode: the process can start, signal, and await workflows but runs no background workers. See [workers & scaling](/workers-and-scaling/). |
| `ConfigureWorkflowWorker(options => ...)` | Tune the workflow worker (see below). |
| `ConfigureActivityWorker(options => ...)` | Tune the activity worker (see below). |
| `AddWorkflow<TWorkflow>()` | Register a workflow class. |
| `AddActivities<TActivities>()` | Register an activity class (all `[Activity]` methods). |
| `RegisterActivity<TInput, TOutput>(...)` | Register a single delegate-based activity. |

<!-- TODO: document the second UsePostgres overload and RegisterActivity variants. -->

## Connection pooling

`UsePostgres` caps the Npgsql connection pool at **20 connections per process**. Npgsql's
own default is 100, which is also Postgres' default `max_connections`, so a few API and
worker processes sharing one database can exhaust it under load and start failing with
`53300: sorry, too many clients already`.

Override the cap per process with the `maxPoolSize` parameter:

```csharp
pg.UsePostgres(connectionString, maxPoolSize: 40)
```

Or in the connection string, which is respected when `maxPoolSize` is not passed:

```text
Host=db;Database=app;Username=app;Maximum Pool Size=40
```

When sizing, budget across the whole fleet: every API and worker process holds its own
pool, and the sum of all pools must stay below the server's `max_connections` with room
to spare for migrations, dashboards, and ad-hoc connections.

If you pass a pre-built `NpgsqlDataSource` to `UsePostgres`, its pool settings are used
as-is and no cap is applied.

## WorkflowWorkerOptions

| Option | Default | Description |
| ------ | ------- | ----------- |
| `WorkerId` | machine name | Identifies this worker in leases. |
| `BatchSize` | `16` | Max runs leased per poll. |
| `MaxConcurrency` | processor count | Max runs executed concurrently. |
| `LeaseDuration` | 30 s | How long a lease lives between heartbeats. |
| `PollInterval` | 250 ms | How often an idle worker polls for work. |
| `ParkGrace` | 30 s | Safety-net deadline for runs parked on activity steps; they're normally woken sooner by the edge-trigger. |
| `MaxAttempts` | `1` | Workflow-level attempts. Transient database errors do not consume an attempt; see [error handling](/error-handling/#infrastructure-errors). |
| `GetRetryDelay` | `min(attempt × 5 s, 60 s)` | Backoff between attempts. Also used as the delay before a run released by a transient database error becomes visible again. |

## ActivityWorkerOptions

| Option | Default | Description |
| ------ | ------- | ----------- |
| `WorkerId` | machine name | Identifies this worker in leases. |
| `BatchSize` | `16` | Max jobs leased per poll; only effective below `MaxConcurrency`. |
| `MaxConcurrency` | processor count × 4 | Max activities executed concurrently. Lower it for CPU-bound work. |
| `LeaseDuration` | 30 s | How long a lease lives between heartbeats. |
| `PollInterval` | 250 ms | How often an idle worker polls for work. |
| `GetRetryDelay` | `min(attempt × 5 s, 60 s)` | Backoff between activity retry attempts. |

<!-- TODO: where does activity MaxAttempts live? Document once decided/confirmed. -->
