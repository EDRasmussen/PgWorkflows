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
| `UsePostgres(connectionString, ensureSchemaOnStart = true, maxPoolSize = null)` | Point PgWorkflows at your database. Creates the schema on startup by default and sizes the connection pool to the worker concurrency unless told otherwise (see [connection pooling](#connection-pooling)). |
| `DisableWorkers()` | Client-only mode: the process can start, signal, and await workflows but runs no background workers. See [workers & scaling](/workers-and-scaling/). |
| `ConfigureWorkflowWorker(options => ...)` | Tune the workflow worker (see below). |
| `ConfigureActivityWorker(options => ...)` | Tune the activity worker (see below). |
| `AddWorkflow<TWorkflow>()` | Register a workflow class. |
| `AddActivities<TActivities>()` | Register an activity class (all `[Activity]` methods). |
| `RegisterActivity<TInput, TOutput>(...)` | Register a single delegate-based activity. |

`UsePostgres` also accepts a pre-built `NpgsqlDataSource` instead of a connection string,
for apps that already manage their own data source; its pool settings are used as-is and
validated against the worker concurrency.

`RegisterActivity` is the low-level escape hatch next to `AddActivities`: it binds a
delegate to an explicit durable name. Overloads accept sync or async delegates, an
optional `CancellationToken`, and an optional `ActivityExecutionContext` first parameter
carrying the job id and attempt number.

## Schema migrations

The schema is versioned. On startup (with `ensureSchemaOnStart: true`, the default),
PgWorkflows checks the `pw_schema_migrations` table and applies only the migrations your
database is missing, in order, inside a transaction guarded by an advisory lock; a whole
fleet starting at once applies them exactly once. Upgrading the package and restarting
is the entire upgrade procedure.

To apply migrations out-of-band instead (a deploy pipeline, a DBA), pass
`ensureSchemaOnStart: false` and use `PostgresSchema.Migrations`: execute each pending
migration's `Sql` and insert its row into `pw_schema_migrations(version, name,
applied_at)` in the same transaction.

## Connection pooling

A worker holds one connection per in-flight item, so the pool size and the worker
concurrency are really the same number. You set the concurrency you want;
`UsePostgres` **sizes the pool to fit it** — `ActivityWorker.MaxConcurrency +
WorkflowWorker.MaxConcurrency` plus a little headroom (a client-only process gets 20).
There is no second knob to reconcile. A single shared heartbeat renews all of a worker's
leases in one query, so renewal does not add a connection per in-flight item.

Override the per-process cap explicitly with the `maxPoolSize` parameter (e.g. to share a
budget across a large fleet); an explicit pool that is too small for the configured
concurrency **fails fast** at startup:

```csharp
pg.UsePostgres(connectionString, maxPoolSize: 40)
```

Or in the connection string, which is respected when `maxPoolSize` is not passed:

```text
Host=db;Database=app;Username=app;Maximum Pool Size=40
```

If you pass a pre-built `NpgsqlDataSource` to `UsePostgres`, its pool settings are used
as-is (and validated against the concurrency, failing fast if too small).

When you do set the pool explicitly, budget across the whole fleet: every API and worker
process holds its own pool, and the sum of all pools must stay below the server's
`max_connections` (Npgsql's and Postgres' default is 100) with room to spare for
migrations, dashboards, and ad-hoc connections.

## WorkflowWorkerOptions

| Option | Default | Description |
| ------ | ------- | ----------- |
| `WorkerId` | machine name | Identifies this worker in leases. |
| `BatchSize` | `16` | Runs leased per database round-trip, capped at `MaxConcurrency`. A round-trip amortization knob, not a concurrency limit; the worker refills slots continuously. |
| `MaxConcurrency` | `10` | Max runs in flight at once. The worker keeps this many running, leasing a replacement the moment one finishes. The connection pool sizes itself to this (plus the activity worker's share) — see [connection pooling](#connection-pooling). |
| `LeaseDuration` | 30 s | How long a lease lives between heartbeats. |
| `PollInterval` | 250 ms | How often an idle worker polls for work. |
| `ParkGrace` | 30 s | Safety-net deadline for runs parked on activity steps; they're normally woken sooner by the edge-trigger. |
| `MaxAttempts` | `1` | Workflow-level attempts. Transient database errors do not consume an attempt; see [error handling](/error-handling/#infrastructure-errors). |
| `GetRetryDelay` | `min(attempt × 5 s, 60 s)` | Backoff between attempts. Also used as the delay before a run released by a transient database error becomes visible again. |

## ActivityWorkerOptions

| Option | Default | Description |
| ------ | ------- | ----------- |
| `WorkerId` | machine name | Identifies this worker in leases. |
| `BatchSize` | `16` | Jobs leased per database round-trip, capped at `MaxConcurrency`. A round-trip amortization knob, not a concurrency limit; the worker refills slots continuously. |
| `MaxConcurrency` | `10` | Max activities in flight at once, refilled the moment one finishes. The connection pool sizes itself to this (plus the workflow worker's share) — see [connection pooling](#connection-pooling). |
| `LeaseDuration` | 30 s | How long a lease lives between heartbeats. |
| `PollInterval` | 250 ms | How often an idle worker polls for work. |
| `GetRetryDelay` | `min(attempt × 5 s, 60 s)` | Backoff between activity retry attempts. |

There is no per-activity retry budget yet: an activity called from a workflow gets a
single execution attempt, and its failure surfaces to the workflow (see
[error handling](/error-handling/)). Configurable activity retries are planned.
