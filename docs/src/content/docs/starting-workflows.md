---
title: Starting workflows
description: Use IPgWorkflowClient to start, await, and signal workflow runs — with idempotency built in.
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

<!-- TODO: what the handle offers — WorkflowRunId, GetResultAsync, SignalAsync — and how
     to signal a run from another process via the client + run id. -->

## Idempotency keys

Pass an idempotency key so a retried producer doesn't start the same workflow twice:

```csharp
var handle = await workflows.StartAsync<TrialOnboardingWorkflow, SignupInput, string>(
    new SignupInput("Acme", "ada@acme.dev"),
    idempotencyKey: $"signup:{signupId}"
);
```

<!-- TODO: semantics — same key returns the existing run instead of creating a new one;
     scope/uniqueness of keys; keys on SignalAsync too. -->
