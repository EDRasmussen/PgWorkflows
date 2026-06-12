---
title: Starting workflows
description: Use IPgWorkflowClient to start, await, and signal workflow runs, with idempotency built in.
---

`IPgWorkflowClient` is how your application code talks to PgWorkflows: start a run and
await its result, or fire-and-track with a handle.

## Execute and wait

When you want the result inline:

```csharp
var result = await workflows.ExecuteAsync<GreetingWorkflow, GreetingInput, string>(
    new GreetingInput("Postgres", 42)
);
```

## Start and track

When the workflow is long-running, start it and keep a handle:

```csharp
var handle = await workflows.StartAsync<TrialOnboardingWorkflow, SignupInput, string>(
    new SignupInput("Acme", "ada@acme.dev")
);

Console.WriteLine(handle.WorkflowRunId);

var result = await handle.GetResultAsync();
```

The handle is cheap and stateless; the run itself lives in Postgres. It offers three
things: `WorkflowRunId` (store it, return it from your API), `GetResultAsync` (waits for
a worker to finish the run and returns the recorded result, throwing if the run failed),
and `SignalAsync` (delivers a [signal](/signals/) to the run).

You don't need to keep the handle around. Any process can signal a run later through the
client and the stored id:

```csharp
await workflows.SignalAsync(workflowRunId, "upgrade", new UpgradeDecision(true, "pro"));
```

## Idempotency keys

Pass an idempotency key so a retried producer doesn't start the same workflow twice:

```csharp
var handle = await workflows.StartAsync<TrialOnboardingWorkflow, SignupInput, string>(
    new SignupInput("Acme", "ada@acme.dev"),
    idempotencyKey: $"signup:{signupId}"
);
```

Starting with a key that was already used returns a handle to the existing run instead
of creating a new one, so a retried HTTP request or a redelivered queue message can't
fork your workflow. Keys are scoped per workflow name: `signup:42` on
`TrialOnboardingWorkflow` and on some other workflow are independent.

`SignalAsync` takes an idempotency key too, with the same effect: a redelivered signal
with a known key buffers nothing and the workflow consumes the payload once.
