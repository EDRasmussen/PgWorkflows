using System.Text.Json;
using PgWorkflows.Persistence;

namespace PgWorkflows.Workflows;

public sealed class WorkflowRunner(
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
            }
            else
            {
                var recorded = await _workflowStore.RecordRunFailureAsync(
                    workflowRunId,
                    ex.ToString(),
                    leaseToken,
                    CancellationToken.None
                );

                if (recorded)
                {
                    return null;
                }
            }

            throw;
        }
    }

    private T Deserialize<T>(string? json) =>
        json is null ? default! : JsonSerializer.Deserialize<T>(json, _jsonSerializerOptions)!;
}
