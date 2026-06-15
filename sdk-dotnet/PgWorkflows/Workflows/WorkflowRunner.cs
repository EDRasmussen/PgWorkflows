using System.Text.Json;
using PgWorkflows.Jobs;
using PgWorkflows.Persistence;

namespace PgWorkflows.Workflows;

internal sealed class WorkflowRunner(
    IWorkflowStore workflowStore,
    IActivityJobStore activityStore,
    JsonSerializerOptions? jsonSerializerOptions = null
)
{
    private readonly IWorkflowStore _workflowStore =
        workflowStore ?? throw new ArgumentNullException(nameof(workflowStore));
    private readonly IActivityJobStore _activityStore =
        activityStore ?? throw new ArgumentNullException(nameof(activityStore));
    private readonly JsonSerializerOptions? _jsonSerializerOptions = jsonSerializerOptions;

    public TimeSpan ActivityPollInterval { get; init; } = TimeSpan.FromMilliseconds(100);

    /// <summary>
    /// Safety-net deadline a leased run is parked for while waiting on activity steps. The run is
    /// normally woken earlier by the edge-trigger when its last outstanding activity job completes;
    /// this grace only bounds how long a parked run lingers if that wake is ever missed (e.g. a crash
    /// between job completion and the wake write). Long enough that idle parked runs are not
    /// effectively polled, short enough to recover promptly.
    /// </summary>
    public TimeSpan ParkGrace { get; init; } = TimeSpan.FromSeconds(30);

    internal async ValueTask<Guid> StartAsync(
        string workflowName,
        object? input,
        Type inputType,
        string? idempotencyKey = null,
        CancellationToken cancellationToken = default
    )
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(workflowName);
        ArgumentNullException.ThrowIfNull(inputType);

        var inputJson = JsonSerializer.Serialize(input, inputType, _jsonSerializerOptions);
        return await _workflowStore.CreateRunAsync(
            new CreateWorkflowRunRequest(workflowName, inputJson, idempotencyKey),
            cancellationToken
        );
    }

    internal async ValueTask RunFailureHooksAsync(
        Guid workflowRunId,
        CancellationToken cancellationToken = default
    )
    {
        var hooks = await _workflowStore.ListFailureHooksAsync(workflowRunId, cancellationToken);
        foreach (var hook in hooks)
        {
            await RunFailureHookAsync(hook, cancellationToken);
        }
    }

    internal async ValueTask<TOutput> WaitForResultAsync<TOutput>(
        Guid workflowRunId,
        CancellationToken cancellationToken = default
    )
    {
        while (true)
        {
            cancellationToken.ThrowIfCancellationRequested();

            var run =
                await _workflowStore.GetRunAsync(workflowRunId, cancellationToken)
                ?? throw new InvalidOperationException(
                    $"Workflow run '{workflowRunId}' was not found."
                );

            if (run.Status == WorkflowStatus.Succeeded)
            {
                return Deserialize<TOutput>(run.ResultJson);
            }

            if (run.Status == WorkflowStatus.Failed)
            {
                throw new InvalidOperationException(
                    $"Workflow run '{workflowRunId}' failed: {run.Error ?? "Unknown error."}"
                );
            }

            await Task.Delay(ActivityPollInterval, cancellationToken);
        }
    }

    internal async ValueTask SignalAsync<TSignal>(
        Guid workflowRunId,
        string name,
        TSignal signal,
        string? idempotencyKey = null,
        CancellationToken cancellationToken = default
    )
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(name);
        // A null payload would deserialize to null in WaitForSignal<TSignal> and surface as an NRE
        // inside the workflow, far from the sender.
        ArgumentNullException.ThrowIfNull(signal);
        var payloadJson = JsonSerializer.Serialize(signal, _jsonSerializerOptions);
        await _workflowStore.RecordSignalAsync(
            workflowRunId,
            name,
            payloadJson,
            idempotencyKey,
            cancellationToken
        );
    }

    internal async ValueTask<WorkflowExecutionOutcome> ExecuteLeasedAsync(
        Guid workflowRunId,
        WorkflowDefinition workflow,
        IServiceProvider serviceProvider,
        string leaseToken,
        CancellationToken cancellationToken = default
    )
    {
        ArgumentNullException.ThrowIfNull(workflow);
        ArgumentNullException.ThrowIfNull(serviceProvider);
        ArgumentException.ThrowIfNullOrWhiteSpace(leaseToken);

        var run =
            await _workflowStore.GetRunAsync(workflowRunId, cancellationToken)
            ?? throw new InvalidOperationException(
                $"Workflow run '{workflowRunId}' was not found."
            );

        var input = run.InputJson is null
            ? null
            : JsonSerializer.Deserialize(run.InputJson, workflow.InputType, _jsonSerializerOptions);
        var context = new WorkflowContext(
            workflowRunId,
            _workflowStore,
            _activityStore,
            _jsonSerializerOptions,
            leaseToken
        );

        try
        {
            var result = await workflow.InvokeAsync(
                serviceProvider,
                context,
                input,
                cancellationToken
            );

            if (context.ParkRequested)
            {
                // The workflow completed despite requesting a park, so user code swallowed the
                // control-flow exception. Fail loudly instead of recording success and skipping the
                // suspend.
                throw new InvalidOperationException(
                    "Workflow completed after it requested a durable park (ctx.Sleep, ctx.WaitForSignal, or an activity "
                        + "wait); its internal control-flow exception was swallowed. Do not wrap "
                        + "ctx.Sleep, ctx.WaitForSignal, ctx.Activity, or ctx.WhenAll in a broad catch."
                );
            }

            var resultJson = JsonSerializer.Serialize(
                result,
                workflow.OutputType,
                _jsonSerializerOptions
            );

            var recorded = await _workflowStore.RecordRunSuccessAsync(
                workflowRunId,
                resultJson,
                leaseToken,
                CancellationToken.None
            );
            return recorded
                ? WorkflowExecutionOutcome.Completed
                : WorkflowExecutionOutcome.LeaseLost;
        }
        catch (WorkflowSleepException sleep)
        {
            var parked = await _workflowStore.RecordRunSleepingAsync(
                workflowRunId,
                sleep.TimerSequence,
                sleep.FireAt,
                leaseToken,
                CancellationToken.None
            );
            return parked ? WorkflowExecutionOutcome.Sleeping : WorkflowExecutionOutcome.LeaseLost;
        }
        catch (WorkflowSignalWaitException signalWait)
        {
            // No grace deadline: a signal may take days, so the run parks open-ended and signal
            // delivery wakes it via the edge-trigger.
            var parked = await _workflowStore.RecordRunWaitingForSignalAsync(
                workflowRunId,
                signalWait.WaitSequence,
                signalWait.SignalName,
                leaseToken,
                CancellationToken.None
            );
            return parked
                ? WorkflowExecutionOutcome.WaitingForSignal
                : WorkflowExecutionOutcome.LeaseLost;
        }
        catch (WorkflowParkException)
        {
            // Waiting on activity steps: park until the edge-trigger wakes it (or ParkGrace elapses).
            // Control flow, not failure; other exceptions propagate to the worker, which records it.
            var parked = await _workflowStore.RecordRunWaitingAsync(
                workflowRunId,
                leaseToken,
                ParkGrace,
                CancellationToken.None
            );
            return parked
                ? WorkflowExecutionOutcome.WaitingForActivities
                : WorkflowExecutionOutcome.LeaseLost;
        }
    }

    private async ValueTask RunFailureHookAsync(
        WorkflowFailureHook hook,
        CancellationToken cancellationToken
    )
    {
        if (hook.Status == WorkflowFailureHookStatus.Succeeded)
        {
            return;
        }

        if (hook.Status == WorkflowFailureHookStatus.Failed)
        {
            throw new InvalidOperationException(
                $"Workflow failure hook {hook.HookSequence} previously failed: {hook.Error}"
            );
        }

        var activityJobId = hook.ActivityJobId;
        if (activityJobId is null)
        {
            activityJobId = await _activityStore.EnqueueAsync(
                hook.ActivityName,
                hook.InputJson,
                idempotencyKey: $"workflow:{hook.WorkflowRunId:N}:failure:{hook.HookSequence}",
                cancellationToken: cancellationToken
            );

            await _workflowStore.RecordFailureHookScheduledAsync(
                hook.WorkflowRunId,
                hook.HookSequence,
                activityJobId.Value,
                cancellationToken
            );
        }

        while (true)
        {
            cancellationToken.ThrowIfCancellationRequested();

            var job =
                await _activityStore.GetAsync(activityJobId.Value, cancellationToken)
                ?? throw new InvalidOperationException(
                    $"Activity job '{activityJobId}' for workflow failure hook {hook.HookSequence} was not found."
                );

            if (job.Status == JobStatus.Succeeded)
            {
                await _workflowStore.RecordFailureHookSuccessAsync(
                    hook.WorkflowRunId,
                    hook.HookSequence,
                    job.ResultJson,
                    cancellationToken
                );
                return;
            }

            if (job.Status == JobStatus.Failed)
            {
                var error = job.Error ?? "Failure hook activity failed.";
                await _workflowStore.RecordFailureHookFailureAsync(
                    hook.WorkflowRunId,
                    hook.HookSequence,
                    error,
                    cancellationToken
                );

                throw new InvalidOperationException(
                    $"Workflow failure hook {hook.HookSequence} failed: {error}"
                );
            }

            await Task.Delay(ActivityPollInterval, cancellationToken);
        }
    }

    private T Deserialize<T>(string? json) =>
        json is null ? default! : JsonSerializer.Deserialize<T>(json, _jsonSerializerOptions)!;
}
