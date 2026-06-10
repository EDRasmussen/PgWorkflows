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
| `UsePostgres(connectionString, ensureSchemaOnStart = true)` | Point PgWorkflows at your database. Creates the schema on startup by default. |
| `DisableWorkers()` | Client-only mode: the process can start, signal, and await workflows but runs no background workers. See [workers & scaling](/workers-and-scaling/). |
| `ConfigureWorkflowWorker(options => ...)` | Tune the workflow worker (see below). |
| `ConfigureActivityWorker(options => ...)` | Tune the activity worker (see below). |
| `AddWorkflow<TWorkflow>()` | Register a workflow class. |
| `AddActivities<TActivities>()` | Register an activity class (all `[Activity]` methods). |
| `RegisterActivity<TInput, TOutput>(...)` | Register a single delegate-based activity. |

<!-- TODO: document the second UsePostgres overload and RegisterActivity variants. -->

## WorkflowWorkerOptions

| Option | Default | Description |
| ------ | ------- | ----------- |
| `WorkerId` | machine name | Identifies this worker in leases. |
| `BatchSize` | `16` | Max runs leased per poll. |
| `MaxConcurrency` | processor count | Max runs executed concurrently. |
| `LeaseDuration` | 30 s | How long a lease lives between heartbeats. |
| `PollInterval` | 250 ms | How often an idle worker polls for work. |
| `ParkGrace` | 30 s | Safety-net deadline for runs parked on activity steps; they're normally woken sooner by the edge-trigger. |
| `MaxAttempts` | `1` | Workflow-level attempts. |
| `GetRetryDelay` | `min(attempt × 5 s, 60 s)` | Backoff between attempts. |

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
