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
    TimeSpan activityPollInterval,
    bool canSleep
) : IWorkflowContext
{
    private readonly IWorkflowStore _workflowStore =
        workflowStore ?? throw new ArgumentNullException(nameof(workflowStore));
    private readonly IActivityJobStore _activityStore =
        activityStore ?? throw new ArgumentNullException(nameof(activityStore));
    private readonly JsonSerializerOptions? _jsonSerializerOptions = jsonSerializerOptions;
    private readonly TimeSpan _activityPollInterval = activityPollInterval;
    private readonly bool _canSleep = canSleep;
    private int _nextStepSequence;
    private int _nextFailureHookSequence;
    private int _nextTimerSequence;

    public Guid WorkflowRunId { get; } = workflowRunId;

    /// <summary>
    /// True once <see cref="Sleep"/> has decided to park (it has thrown
    /// <see cref="WorkflowSleepException"/>). If the workflow nevertheless completes normally, the
    /// park exception was swallowed by user code and the runner must fail loudly instead of
    /// recording success.
    /// </summary>
    internal bool ParkRequested { get; private set; }

    public async ValueTask Sleep(TimeSpan duration, CancellationToken cancellationToken = default)
    {
        if (!_canSleep)
        {
            throw new NotSupportedException(
                "ctx.Sleep is only supported when the workflow is executed by the workflow worker, "
                    + "not when executed inline in the caller."
            );
        }

        var timerSequence = _nextTimerSequence++;

        // Read-only: the deadline is persisted atomically with the park (RecordRunSleepingAsync),
        // so a first encounter computes the deadline and throws, and the runner durably writes it.
        var fireAt = await _workflowStore.GetTimerAsync(
            WorkflowRunId,
            timerSequence,
            cancellationToken
        );

        if (fireAt is null)
        {
            var delay = duration > TimeSpan.Zero ? duration : TimeSpan.Zero;
            fireAt = DateTimeOffset.UtcNow.Add(delay);
        }

        if (DateTimeOffset.UtcNow >= fireAt.Value)
        {
            // Already elapsed (including zero/negative durations): continue without parking or
            // persisting, so no orphan timer row is created.
            return;
        }

        ParkRequested = true;
        throw new WorkflowSleepException(timerSequence, fireAt.Value);
    }

    public WorkflowActivity<TOutput> CallActivity<TActivities, TOutput>(
        Expression<Func<TActivities, TOutput>> activityCall
    ) =>
        new(WorkflowActivityCall.FromExpression(activityCall, _jsonSerializerOptions));

    public WorkflowActivity<TOutput> CallActivity<TActivities, TOutput>(
        Expression<Func<TActivities, Task<TOutput>>> activityCall
    ) =>
        new(WorkflowActivityCall.FromExpression(activityCall, _jsonSerializerOptions));

    public WorkflowActivity<TOutput> CallActivity<TActivities, TOutput>(
        Expression<Func<TActivities, ValueTask<TOutput>>> activityCall
    ) =>
        new(WorkflowActivityCall.FromExpression(activityCall, _jsonSerializerOptions));

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

    public ValueTask OnFailure<TActivities>(
        Expression<Action<TActivities>> activityCall,
        CancellationToken cancellationToken = default
    ) =>
        RegisterFailureHookAsync(
            WorkflowActivityCall.FromExpression(activityCall, _jsonSerializerOptions),
            cancellationToken
        );

    public ValueTask OnFailure<TActivities>(
        Expression<Func<TActivities, Task>> activityCall,
        CancellationToken cancellationToken = default
    ) =>
        RegisterFailureHookAsync(
            WorkflowActivityCall.FromExpression(activityCall, _jsonSerializerOptions),
            cancellationToken
        );

    public ValueTask OnFailure<TActivities>(
        Expression<Func<TActivities, ValueTask>> activityCall,
        CancellationToken cancellationToken = default
    ) =>
        RegisterFailureHookAsync(
            WorkflowActivityCall.FromExpression(activityCall, _jsonSerializerOptions),
            cancellationToken
        );

    public ValueTask OnFailure<TActivities, TOutput>(
        Expression<Func<TActivities, TOutput>> activityCall,
        CancellationToken cancellationToken = default
    ) =>
        RegisterFailureHookAsync(
            WorkflowActivityCall.FromExpression(activityCall, _jsonSerializerOptions),
            cancellationToken
        );

    public ValueTask OnFailure<TActivities, TOutput>(
        Expression<Func<TActivities, Task<TOutput>>> activityCall,
        CancellationToken cancellationToken = default
    ) =>
        RegisterFailureHookAsync(
            WorkflowActivityCall.FromExpression(activityCall, _jsonSerializerOptions),
            cancellationToken
        );

    public ValueTask OnFailure<TActivities, TOutput>(
        Expression<Func<TActivities, ValueTask<TOutput>>> activityCall,
        CancellationToken cancellationToken = default
    ) =>
        RegisterFailureHookAsync(
            WorkflowActivityCall.FromExpression(activityCall, _jsonSerializerOptions),
            cancellationToken
        );

    public async ValueTask<TOutput[]> WhenAll<TOutput>(
        IEnumerable<WorkflowActivity<TOutput>> activities,
        CancellationToken cancellationToken = default
    )
    {
        ArgumentNullException.ThrowIfNull(activities);

        var activityCalls = activities.Select(GetCall).ToArray();
        if (activityCalls.Length == 0)
        {
            return [];
        }

        var firstStepSequence = ReserveStepSequences(activityCalls.Length);
        var tasks = new Task<TOutput>[activityCalls.Length];
        for (var index = 0; index < activityCalls.Length; index++)
        {
            tasks[index] = RunActivityStepAsync<TOutput>(
                firstStepSequence + index,
                activityCalls[index],
                cancellationToken
            ).AsTask();
        }

        return await Task.WhenAll(tasks);
    }

    public async ValueTask<(T1 First, T2 Second)> WhenAll<T1, T2>(
        WorkflowActivity<T1> first,
        WorkflowActivity<T2> second,
        CancellationToken cancellationToken = default
    )
    {
        var firstStepSequence = ReserveStepSequences(2);
        var firstTask = RunActivityStepAsync<T1>(
            firstStepSequence,
            GetCall(first),
            cancellationToken
        ).AsTask();
        var secondTask = RunActivityStepAsync<T2>(
            firstStepSequence + 1,
            GetCall(second),
            cancellationToken
        ).AsTask();

        await Task.WhenAll(firstTask, secondTask);
        return (await firstTask, await secondTask);
    }

    public async ValueTask<(T1 First, T2 Second, T3 Third)> WhenAll<T1, T2, T3>(
        WorkflowActivity<T1> first,
        WorkflowActivity<T2> second,
        WorkflowActivity<T3> third,
        CancellationToken cancellationToken = default
    )
    {
        var firstStepSequence = ReserveStepSequences(3);
        var firstTask = RunActivityStepAsync<T1>(
            firstStepSequence,
            GetCall(first),
            cancellationToken
        ).AsTask();
        var secondTask = RunActivityStepAsync<T2>(
            firstStepSequence + 1,
            GetCall(second),
            cancellationToken
        ).AsTask();
        var thirdTask = RunActivityStepAsync<T3>(
            firstStepSequence + 2,
            GetCall(third),
            cancellationToken
        ).AsTask();

        await Task.WhenAll(firstTask, secondTask, thirdTask);
        return (await firstTask, await secondTask, await thirdTask);
    }

    private async ValueTask<TOutput> RunActivityStepAsync<TOutput>(
        WorkflowActivityCall activityCall,
        CancellationToken cancellationToken
    )
    {
        var stepSequence = ReserveStepSequences(1);
        return await RunActivityStepAsync<TOutput>(stepSequence, activityCall, cancellationToken);
    }

    private async ValueTask<TOutput> RunActivityStepAsync<TOutput>(
        int stepSequence,
        WorkflowActivityCall activityCall,
        CancellationToken cancellationToken
    )
    {
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
                activityCall.ActivityName,
                activityCall.InputJson,
                idempotencyKey: $"workflow:{WorkflowRunId:N}:{stepSequence}",
                cancellationToken: cancellationToken
            );

            await _workflowStore.RecordStepScheduledAsync(
                WorkflowRunId,
                stepSequence,
                activityCall.ActivityName,
                activityJobId.Value,
                activityCall.InputJson,
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

    private int ReserveStepSequences(int count)
    {
        var firstStepSequence = _nextStepSequence;
        _nextStepSequence += count;
        return firstStepSequence;
    }

    private async ValueTask RegisterFailureHookAsync(
        WorkflowActivityCall activityCall,
        CancellationToken cancellationToken
    )
    {
        var hookSequence = _nextFailureHookSequence++;
        await _workflowStore.RecordFailureHookRegisteredAsync(
            WorkflowRunId,
            hookSequence,
            activityCall.ActivityName,
            activityCall.InputJson,
            cancellationToken
        );
    }

    private static WorkflowActivityCall GetCall<TOutput>(WorkflowActivity<TOutput> activity) =>
        activity.Call
        ?? throw new InvalidOperationException(
            "Workflow activities passed to WhenAll must be created by IWorkflowContext.CallActivity."
        );

    private TOutput DeserializeResult<TOutput>(string? resultJson) =>
        resultJson is null
            ? default!
            : JsonSerializer.Deserialize<TOutput>(resultJson, _jsonSerializerOptions)!;
}
