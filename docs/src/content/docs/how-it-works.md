---
title: How it works
description: The mental model behind PgWorkflows — durable steps, leases, and parked runs, all in Postgres tables.
---

PgWorkflows turns ordinary async C# methods into durable workflows by persisting every
step's outcome in Postgres. No separate server, no event-history replay rules — just
your app and a handful of tables.

## The mental model

<!-- TODO: a workflow is a plain async method; every ctx.* call is a durable step.
     If the process dies, another worker picks the run up and already-completed steps
     return their stored results instead of re-running. -->

## Durable steps

<!-- TODO: explain step memoization — each ctx.Activity / ctx.Sleep / ctx.WaitForSignal
     is keyed per run, results are persisted, side effects happen exactly once. -->

## Workers and leases

<!-- TODO: polling workers lease work with FOR UPDATE SKIP LOCKED, heartbeat the lease
     while running, and reclaim leases from dead workers. Scale out by running more
     instances of your app. -->

## Parked runs

<!-- TODO: Sleep and WaitForSignal don't hold a thread or a worker slot — the run is
     parked in the database and woken by a timer or signal, surviving restarts and
     deploys. Note the caveat: parking works via an internal control-flow exception, so
     don't wrap ctx.Sleep / ctx.WaitForSignal in a broad catch. -->

## What's in your database

<!-- TODO: short tour of the tables PgWorkflows creates and what each row means.
     Reassure: it's all inspectable with plain SQL. -->
