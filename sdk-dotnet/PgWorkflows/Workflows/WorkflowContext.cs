using System.Linq.Expressions;
using System.Text.Json;
using PgWorkflows.Jobs;
using PgWorkflows.Persistence;

namespace PgWorkflows.Workflows;

internal sealed class WorkflowContext(
    Guid workflowRunId,
    IWorkflowStore workflowStore,
    IActivityJobStore activityStore,
    JsonSerializerOptions? jsonSerializerOptions,
    TimeSpan activityPollInterval
) : IWorkflowContext
{
    private readonly IWorkflowStore _workflowStore =
        workflowStore ?? throw new ArgumentNullException(nameof(workflowStore));
    private readonly IActivityJobStore _activityStore =
        activityStore ?? throw new ArgumentNullException(nameof(activityStore));
    private readonly JsonSerializerOptions? _jsonSerializerOptions = jsonSerializerOptions;
    private readonly TimeSpan _activityPollInterval = activityPollInterval;
    private int _nextStepSequence;

    public Guid WorkflowRunId { get; } = workflowRunId;

    public ValueTask<TOutput> Activity<TActivities, TOutput>(
        Expression<Func<TActivities, TOutput>> activityCall,
        CancellationToken cancellationToken = default
    ) =>
        RunActivityStepAsync<TOutput>(
            WorkflowActivityCall.FromExpression(activityCall, _jsonSerializerOptions),
            cancellationToken
        );

    public ValueTask<TOutput> Activity<TActivities, TOutput>(
        Expression<Func<TActivities, Task<TOutput>>> activityCall,
        CancellationToken cancellationToken = default
    ) =>
        RunActivityStepAsync<TOutput>(
            WorkflowActivityCall.FromExpression(activityCall, _jsonSerializerOptions),
            cancellationToken
        );

    public ValueTask<TOutput> Activity<TActivities, TOutput>(
        Expression<Func<TActivities, ValueTask<TOutput>>> activityCall,
        CancellationToken cancellationToken = default
    ) =>
        RunActivityStepAsync<TOutput>(
            WorkflowActivityCall.FromExpression(activityCall, _jsonSerializerOptions),
            cancellationToken
        );

    private async ValueTask<TOutput> RunActivityStepAsync<TOutput>(
        WorkflowActivityCall activityCall,
        CancellationToken cancellationToken
    )
    {
        var stepSequence = _nextStepSequence++;
        var step = await _workflowStore.GetStepAsync(
            WorkflowRunId,
            stepSequence,
            cancellationToken
        );

        if (step is { Status: WorkflowStepStatus.Succeeded })
        {
            return DeserializeResult<TOutput>(step.ResultJson);
        }

        if (step is { Status: WorkflowStepStatus.Failed })
        {
            throw new InvalidOperationException(
                $"Workflow activity step {stepSequence} previously failed: {step.Error}"
            );
        }

        var activityJobId = step?.ActivityJobId;
        if (activityJobId is null)
        {
            activityJobId = await _activityStore.EnqueueAsync(
                new EnqueueActivityRequest(
                    activityCall.ActivityName,
                    activityCall.InputJson,
                    IdempotencyKey: $"workflow:{WorkflowRunId:N}:{stepSequence}"
                ),
                cancellationToken
            );

            await _workflowStore.RecordStepScheduledAsync(
                new RecordWorkflowStepRequest(
                    WorkflowRunId,
                    stepSequence,
                    activityCall.ActivityName,
                    activityJobId.Value,
                    activityCall.InputJson
                ),
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
                    $"Activity job '{activityJobId}' for workflow step {stepSequence} was not found."
                );
            }

            if (job.Status == JobStatus.Succeeded)
            {
                await _workflowStore.RecordStepSuccessAsync(
                    WorkflowRunId,
                    stepSequence,
                    job.ResultJson,
                    cancellationToken
                );
                return DeserializeResult<TOutput>(job.ResultJson);
            }

            if (job.Status == JobStatus.Failed)
            {
                var error = job.Error ?? "Activity failed.";
                await _workflowStore.RecordStepFailureAsync(
                    WorkflowRunId,
                    stepSequence,
                    error,
                    cancellationToken
                );
                throw new InvalidOperationException(
                    $"Workflow activity step {stepSequence} failed: {error}"
                );
            }

            await Task.Delay(_activityPollInterval, cancellationToken);
        }
    }

    private TOutput DeserializeResult<TOutput>(string? resultJson) =>
        resultJson is null
            ? default!
            : JsonSerializer.Deserialize<TOutput>(resultJson, _jsonSerializerOptions)!;
}
