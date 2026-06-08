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

    internal async ValueTask<TOutput> ExecuteAsync<TOutput>(
        Guid workflowRunId,
        WorkflowDefinition workflow,
        IServiceProvider serviceProvider,
        CancellationToken cancellationToken = default
    ) =>
        Deserialize<TOutput>(
            await ExecuteCoreAsync(
                workflowRunId,
                workflow,
                serviceProvider,
                leaseToken: null,
                cancellationToken
            )
        );

    internal async ValueTask ExecuteLeasedAsync(
        Guid workflowRunId,
        WorkflowDefinition workflow,
        IServiceProvider serviceProvider,
        string leaseToken,
        CancellationToken cancellationToken = default
    ) =>
        await ExecuteCoreAsync(workflowRunId, workflow, serviceProvider, leaseToken, cancellationToken);

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

            var run = await _workflowStore.GetRunAsync(workflowRunId, cancellationToken)
                ?? throw new InvalidOperationException($"Workflow run '{workflowRunId}' was not found.");

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

    private async ValueTask<string?> ExecuteCoreAsync(
        Guid workflowRunId,
        WorkflowDefinition workflow,
        IServiceProvider serviceProvider,
        string? leaseToken,
        CancellationToken cancellationToken
    )
    {
        ArgumentNullException.ThrowIfNull(workflow);
        ArgumentNullException.ThrowIfNull(serviceProvider);

        if (leaseToken is not null)
        {
            ArgumentException.ThrowIfNullOrWhiteSpace(leaseToken);
        }

        var run = await _workflowStore.GetRunAsync(workflowRunId, cancellationToken)
            ?? throw new InvalidOperationException($"Workflow run '{workflowRunId}' was not found.");

        if (run.Status == WorkflowStatus.Succeeded)
        {
            return run.ResultJson;
        }

        if (leaseToken is null)
        {
            await _workflowStore.MarkRunRunningAsync(workflowRunId, cancellationToken);
        }

        var input = run.InputJson is null
            ? null
            : JsonSerializer.Deserialize(run.InputJson, workflow.InputType, _jsonSerializerOptions);
        var context = new WorkflowContext(
            workflowRunId,
            _workflowStore,
            _activityStore,
            _jsonSerializerOptions,
            ActivityPollInterval
        );

        try
        {
            var result = await workflow.InvokeAsync(
                serviceProvider,
                context,
                input,
                cancellationToken
            );
            var resultJson = JsonSerializer.Serialize(result, workflow.OutputType, _jsonSerializerOptions);
            if (leaseToken is null)
            {
                await _workflowStore.RecordRunSuccessAsync(workflowRunId, resultJson, cancellationToken);
            }
            else
            {
                await _workflowStore.RecordRunSuccessAsync(
                    workflowRunId,
                    resultJson,
                    leaseToken,
                    CancellationToken.None
                );
            }

            return resultJson;
        }
        catch (OperationCanceledException) when (cancellationToken.IsCancellationRequested)
        {
            throw;
        }
        catch (Exception ex)
        {
            if (leaseToken is null)
            {
                await _workflowStore.RecordRunFailureAsync(workflowRunId, ex.ToString(), CancellationToken.None);
                throw;
            }

            throw;
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

            var job = await _activityStore.GetAsync(activityJobId.Value, cancellationToken);
            if (job is null)
            {
                throw new InvalidOperationException(
                    $"Activity job '{activityJobId}' for workflow failure hook {hook.HookSequence} was not found."
                );
            }

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
