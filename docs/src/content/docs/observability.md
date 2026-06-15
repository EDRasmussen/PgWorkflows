---
title: Observability
description: Structured logging, OpenTelemetry tracing, and a dashboard for watching runs.
---

Everything PgWorkflows does is observable three ways: structured logs through your
existing `ILogger`, OpenTelemetry spans for tracing, and plain rows in Postgres that the
dashboard (or `psql`) can show you.

## Logging

Workers log through `Microsoft.Extensions.Logging`, so PgWorkflows shows up in whatever
logging you already have. Worker start and stop, leases, executions, retries, terminal
failures, and lease losses are all structured events with the run or job id attached.

## Tracing

Spans are emitted from a single `ActivitySource` named `PgWorkflows`. Subscribe to it in
your OpenTelemetry setup:

```csharp
builder.Services.AddOpenTelemetry()
    .WithTracing(tracing => tracing.AddSource(PgWorkflowsTelemetry.ActivitySourceName));
```

Two span types cover the work:

| Span | Wraps | Tags |
| ---- | ----- | ---- |
| `pgworkflows.activity.execute` | one activity execution on a worker | `pgworkflows.activity.name`, `pgworkflows.job.id`, `pgworkflows.activity.attempt`, `pgworkflows.activity.max_attempts`, `pgworkflows.worker.id` |
| `pgworkflows.workflow.execute` | one workflow pass on a worker | `pgworkflows.workflow.name`, `pgworkflows.workflow.run_id`, `pgworkflows.workflow.attempt`, `pgworkflows.workflow.max_attempts`, `pgworkflows.workflow.outcome`, `pgworkflows.worker.id` |

A workflow pass is one lease: from picking the run up to completing or parking it. The
`outcome` tag tells you which: `completed`, `sleeping`, `waiting_for_signal`,
`waiting_for_activities`, or `lease_lost`. A long-running workflow therefore appears as
a series of short passes, each ending in a park, rather than one span that lasts days.

## The dashboard

A read-only web UI over the PgWorkflows tables (a live run feed plus a per-run view of
steps, timers, signal waits, and failure hooks), shipped as a Docker image. See
[Dashboard](/dashboard/) to run it.

## Plain SQL

When in doubt, the tables are the truth. See
[what's in your database](/how-it-works/#whats-in-your-database) for the schema tour;
`select * from pw_workflow_runs where status != 'succeeded'` is a complete picture of
your system.
