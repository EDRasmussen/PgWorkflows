---
title: How it works
description: The mental model behind PgWorkflows. Durable steps, leases, and parked runs, all in Postgres tables.
---

PgWorkflows turns ordinary async C# methods into durable workflows by persisting every
step's outcome in Postgres. There is no separate server and there are no event-history
replay rules to learn. Your app and a handful of tables do all the work.

## The mental model

A workflow is a plain async method. Every `ctx.*` call inside it is a durable step: the
step's result is written to Postgres before the workflow moves on. If the process dies
(a crash, a deploy), another worker picks the run up and calls the method again from
the top. Steps that already completed return their stored results instantly instead of
re-running, so execution fast-forwards to where it left off and continues.

That replay is the whole trick. You write straight-line code with `if`s, loops, and
`try`/`catch`; PgWorkflows makes it resumable by remembering what each step returned.
The only rule is that your workflow must reach its `ctx.*` calls in a stable order, which
ordinary deterministic code does naturally.

## Durable steps

Each `ctx.Activity`, `ctx.WhenAll` branch, `ctx.Sleep`, and `ctx.WaitForSignal` is keyed
by its position in the run. The first time a step executes, its outcome is persisted;
on every replay the stored outcome is returned instead. The side effect inside an
activity therefore happens exactly once per run, no matter how many times the run is
resumed.

This is why side effects belong in activities. The workflow method itself may re-execute
many times across resumes; activity bodies do not.

## Workers and leases

Workers poll Postgres for runnable work and claim it with `FOR UPDATE SKIP LOCKED`, so
two workers can never lease the same run or job. A lease is a token plus an expiry: the
worker heartbeats the expiry forward while it works, and every write it makes is guarded
by the token.

That guard is what makes crashes safe. A worker that dies stops heartbeating, its lease
expires, and a peer reclaims the work and resumes from the last completed step. If the
original worker was only frozen and comes back, its token no longer matches and its
writes are rejected, so a stale worker cannot corrupt state. Scaling out is just running
more instances of your app; Postgres is the only coordinator.

## Parked runs

A workflow that waits on an activity, a timer, or a signal does not hold a thread, a
worker slot, or any memory. The run is *parked*: its row is made invisible until a
wake-up condition, and the lease is released. When the activity completes, the timer
fires, or the signal arrives, the row becomes visible again and the next free worker
resumes the run. Parked runs survive restarts and deploys, and a million sleeping
workflows cost you nothing but table rows.

Parking works by throwing an internal control-flow exception that unwinds the workflow
method, so don't wrap `ctx.Sleep`, `ctx.WaitForSignal`, `ctx.Activity`, or `ctx.WhenAll`
in a broad `catch`. If user code swallows the park, the run fails with a clear error
instead of recording a wrong result.

## What's in your database

Everything is plain rows you can inspect with `psql`:

| Table | One row per |
| ----- | ----------- |
| `pw_workflow_runs` | workflow run: status, input, result, error, attempt budget, lease |
| `pw_workflow_steps` | durable activity step: which job backs it, its memoized result |
| `pw_activity_jobs` | activity execution: the queue workers lease from |
| `pw_workflow_timers` | `ctx.Sleep` deadline, persisted so replays don't restart the clock |
| `pw_workflow_signals` | delivered signal payload, consumed in FIFO order per name |
| `pw_workflow_signal_waits` | a workflow's pending or completed `WaitForSignal` |
| `pw_workflow_failure_hooks` | registered `ctx.OnFailure` compensation and its outcome |
| `pw_schema_migrations` | applied schema version, so upgrades know what to run |

When something looks stuck, `select * from pw_workflow_runs where status != 'succeeded'`
is a complete picture of your system. No black boxes.
