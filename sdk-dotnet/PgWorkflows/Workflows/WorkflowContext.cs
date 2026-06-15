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
    string leaseToken
) : IWorkflowContext
{
    private readonly IWorkflowStore _workflowStore =
        workflowStore ?? throw new ArgumentNullException(nameof(workflowStore));
    private readonly IActivityJobStore _activityStore =
        activityStore ?? throw new ArgumentNullException(nameof(activityStore));
    private readonly JsonSerializerOptions? _jsonSerializerOptions = jsonSerializerOptions;

    /// <summary>
    /// The run's lease token under the executing workflow worker. Parking the run (for
    /// <c>ctx.Sleep</c>, <c>ctx.WaitForSignal</c>, and while waiting on activity steps) releases
    /// this lease, guarded by the token so a lost lease writes nothing.
    /// </summary>
    private readonly string _leaseToken =
        leaseToken ?? throw new ArgumentNullException(nameof(leaseToken));
    private int _nextStepSequence;
    private int _nextFailureHookSequence;
    private int _nextTimerSequence;
    private int _nextSignalSequence;

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
        var timerSequence = _nextTimerSequence++;

        // Read-only: the deadline is computed here but persisted atomically with the park
        // (RecordRunSleepingAsync), then replayed on resume.
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
            // Already elapsed (incl. zero/negative): continue without parking, so no orphan timer row.
            return;
        }

        ParkRequested = true;
        throw new WorkflowSleepException(timerSequence, fireAt.Value);
    }

    public async ValueTask<TSignal> WaitForSignal<TSignal>(
        string name,
        CancellationToken cancellationToken = default
    )
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(name);

        var signalSequence = _nextSignalSequence++;
        var payloadJson = await _workflowStore.ConsumeSignalAsync(
            WorkflowRunId,
            signalSequence,
            name,
            _leaseToken,
            cancellationToken
        );

        if (payloadJson is not null)
        {
            return JsonSerializer.Deserialize<TSignal>(payloadJson, _jsonSerializerOptions)!;
        }

        ParkRequested = true;
        throw new WorkflowSignalWaitException(signalSequence, name);
    }

    public WorkflowActivity<TOutput> CallActivity<TActivities, TOutput>(
        Expression<Func<TActivities, TOutput>> activityCall
    ) => new(WorkflowActivityCall.FromExpression(activityCall, _jsonSerializerOptions));

    public WorkflowActivity<TOutput> CallActivity<TActivities, TOutput>(
        Expression<Func<TActivities, Task<TOutput>>> activityCall
    ) => new(WorkflowActivityCall.FromExpression(activityCall, _jsonSerializerOptions));

    public WorkflowActivity<TOutput> CallActivity<TActivities, TOutput>(
        Expression<Func<TActivities, ValueTask<TOutput>>> activityCall
    ) => new(WorkflowActivityCall.FromExpression(activityCall, _jsonSerializerOptions));

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
        var handles = await ScheduleFanOutAsync(
            firstStepSequence,
            activityCalls,
            cancellationToken
        );

        var resolutions = new StepResolution<TOutput>[handles.Length];
        var outcomes = new StepOutcome[handles.Length];
        for (var index = 0; index < handles.Length; index++)
        {
            resolutions[index] = await ResolveStepAsync<TOutput>(handles[index], cancellationToken);
            outcomes[index] = resolutions[index].ToOutcome(handles[index].Sequence);
        }

        // Parks if any sibling is outstanding; else records all and throws the lowest-sequence failure.
        await CommitFanOutAsync(outcomes, cancellationToken);

        var results = new TOutput[handles.Length];
        for (var index = 0; index < handles.Length; index++)
        {
            results[index] = resolutions[index].Value;
        }

        return results;
    }

    public async ValueTask<(T1 First, T2 Second)> WhenAll<T1, T2>(
        WorkflowActivity<T1> first,
        WorkflowActivity<T2> second,
        CancellationToken cancellationToken = default
    )
    {
        var firstStepSequence = ReserveStepSequences(2);
        var handles = await ScheduleFanOutAsync(
            firstStepSequence,
            [GetCall(first), GetCall(second)],
            cancellationToken
        );

        var first2 = await ResolveStepAsync<T1>(handles[0], cancellationToken);
        var second2 = await ResolveStepAsync<T2>(handles[1], cancellationToken);
        await CommitFanOutAsync(
            [first2.ToOutcome(handles[0].Sequence), second2.ToOutcome(handles[1].Sequence)],
            cancellationToken
        );
        return (first2.Value, second2.Value);
    }

    public async ValueTask<(T1 First, T2 Second, T3 Third)> WhenAll<T1, T2, T3>(
        WorkflowActivity<T1> first,
        WorkflowActivity<T2> second,
        WorkflowActivity<T3> third,
        CancellationToken cancellationToken = default
    )
    {
        var firstStepSequence = ReserveStepSequences(3);
        var handles = await ScheduleFanOutAsync(
            firstStepSequence,
            [GetCall(first), GetCall(second), GetCall(third)],
            cancellationToken
        );

        var first3 = await ResolveStepAsync<T1>(handles[0], cancellationToken);
        var second3 = await ResolveStepAsync<T2>(handles[1], cancellationToken);
        var third3 = await ResolveStepAsync<T3>(handles[2], cancellationToken);
        await CommitFanOutAsync(
            [
                first3.ToOutcome(handles[0].Sequence),
                second3.ToOutcome(handles[1].Sequence),
                third3.ToOutcome(handles[2].Sequence),
            ],
            cancellationToken
        );
        return (first3.Value, second3.Value, third3.Value);
    }

    private async ValueTask<TOutput> RunActivityStepAsync<TOutput>(
        WorkflowActivityCall activityCall,
        CancellationToken cancellationToken
    )
    {
        var stepSequence = ReserveStepSequences(1);
        var handle = await EnsureStepScheduledAsync(stepSequence, activityCall, cancellationToken);

        var resolution = await ResolveStepAsync<TOutput>(handle, cancellationToken);
        if (!resolution.Ready)
        {
            // Outstanding: park until the step completes; the workflow replays on resume and this
            // step's result is then memoized.
            Park();
        }

        await FinalizeOutcomeAsync(resolution.ToOutcome(handle.Sequence), cancellationToken);
        if (resolution.Error is { } error)
        {
            throw new InvalidOperationException(
                $"Workflow activity step {handle.Sequence} failed: {error}"
            );
        }

        return resolution.Value;
    }

    /// <summary>
    /// Ensures a step's activity job is enqueued and step row recorded (idempotent on replay).
    /// Never parks or throws on failure; that is the resolve phase's job.
    /// </summary>
    private async ValueTask<StepHandle> EnsureStepScheduledAsync(
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

        if (step is not null)
        {
            return new StepHandle(stepSequence, step.ActivityJobId, step);
        }

        var activityJobId = await _activityStore.EnqueueAsync(
            activityCall.ActivityName,
            activityCall.InputJson,
            idempotencyKey: $"workflow:{WorkflowRunId:N}:{stepSequence}",
            workflowRunId: WorkflowRunId,
            cancellationToken: cancellationToken
        );

        await _workflowStore.RecordStepScheduledAsync(
            WorkflowRunId,
            stepSequence,
            activityCall.ActivityName,
            activityJobId,
            activityCall.InputJson,
            cancellationToken
        );

        return new StepHandle(stepSequence, activityJobId, Step: null);
    }

    /// <summary>
    /// Reads a step's current outcome from its memoized step row, else its activity job; this is
    /// purely observational. Returns the outcome rather than acting on it so a fan-out can wait for all
    /// siblings before committing, matching <see cref="Task.WhenAll(System.Threading.Tasks.Task[])"/>.
    /// </summary>
    private async ValueTask<StepResolution<TOutput>> ResolveStepAsync<TOutput>(
        StepHandle handle,
        CancellationToken cancellationToken
    )
    {
        if (handle.Step is { Status: WorkflowStepStatus.Succeeded } succeededStep)
        {
            return StepResolution<TOutput>.Succeeded(
                DeserializeResult<TOutput>(succeededStep.ResultJson),
                succeededStep.ResultJson
            );
        }

        if (handle.Step is { Status: WorkflowStepStatus.Failed } failedStep)
        {
            return StepResolution<TOutput>.Failed(failedStep.Error ?? "Activity failed.");
        }

        var job =
            await _activityStore.GetAsync(handle.JobId, cancellationToken)
            ?? throw new InvalidOperationException(
                $"Activity job '{handle.JobId}' for workflow step {handle.Sequence} was not found."
            );

        return job.Status switch
        {
            JobStatus.Succeeded => StepResolution<TOutput>.Succeeded(
                DeserializeResult<TOutput>(job.ResultJson),
                job.ResultJson
            ),
            JobStatus.Failed => StepResolution<TOutput>.Failed(job.Error ?? "Activity failed."),
            _ => StepResolution<TOutput>.Pending(),
        };
    }

    /// <summary>
    /// Schedules a whole fan-out: reads existing step rows, then enqueues every not-yet-scheduled job
    /// before recording any step row, so siblings land together and a worker can co-schedule them
    /// instead of leasing only a subset. Idempotent on replay: an existing step row reuses its job.
    /// </summary>
    private async ValueTask<StepHandle[]> ScheduleFanOutAsync(
        int firstStepSequence,
        WorkflowActivityCall[] activityCalls,
        CancellationToken cancellationToken
    )
    {
        var existing = new WorkflowStep?[activityCalls.Length];
        for (var index = 0; index < activityCalls.Length; index++)
        {
            existing[index] = await _workflowStore.GetStepAsync(
                WorkflowRunId,
                firstStepSequence + index,
                cancellationToken
            );
        }

        var jobIds = new Guid[activityCalls.Length];
        var newlyEnqueued = new bool[activityCalls.Length];
        var enqueueTasks = new Task<Guid>?[activityCalls.Length];
        for (var index = 0; index < activityCalls.Length; index++)
        {
            if (existing[index] is { } step)
            {
                jobIds[index] = step.ActivityJobId;
                continue;
            }

            // Enqueue siblings concurrently so they land together and a worker's batch poll can
            // co-schedule them, rather than leasing only the first while the rest are still inserting.
            enqueueTasks[index] = _activityStore
                .EnqueueAsync(
                    activityCalls[index].ActivityName,
                    activityCalls[index].InputJson,
                    idempotencyKey: $"workflow:{WorkflowRunId:N}:{firstStepSequence + index}",
                    workflowRunId: WorkflowRunId,
                    cancellationToken: cancellationToken
                )
                .AsTask();
            newlyEnqueued[index] = true;
        }

        for (var index = 0; index < activityCalls.Length; index++)
        {
            if (enqueueTasks[index] is { } enqueueTask)
            {
                jobIds[index] = await enqueueTask;
            }
        }

        var handles = new StepHandle[activityCalls.Length];
        for (var index = 0; index < activityCalls.Length; index++)
        {
            if (newlyEnqueued[index])
            {
                await _workflowStore.RecordStepScheduledAsync(
                    WorkflowRunId,
                    firstStepSequence + index,
                    activityCalls[index].ActivityName,
                    jobIds[index],
                    activityCalls[index].InputJson,
                    cancellationToken
                );
            }

            handles[index] = new StepHandle(
                firstStepSequence + index,
                jobIds[index],
                existing[index]
            );
        }

        return handles;
    }

    /// <summary>
    /// Commits a resolved fan-out: parks if any sibling is still outstanding, otherwise records every
    /// outcome and throws the lowest-sequence failure if any. Outcomes arrive in step-sequence order,
    /// so the first failure encountered is the lowest sequence.
    /// </summary>
    private async ValueTask CommitFanOutAsync(
        StepOutcome[] outcomes,
        CancellationToken cancellationToken
    )
    {
        foreach (var outcome in outcomes)
        {
            if (!outcome.Ready)
            {
                Park();
            }
        }

        StepOutcome? failure = null;
        foreach (var outcome in outcomes)
        {
            await FinalizeOutcomeAsync(outcome, cancellationToken);
            if (outcome.Error is not null && failure is null)
            {
                failure = outcome;
            }
        }

        if (failure is { } first)
        {
            throw new InvalidOperationException(
                $"Workflow activity step {first.Sequence} failed: {first.Error}"
            );
        }
    }

    /// <summary>
    /// Durably records a resolved (non-pending) step's outcome so it is memoized across replays.
    /// Idempotent: re-recording an already-terminal step is harmless.
    /// </summary>
    private ValueTask FinalizeOutcomeAsync(
        StepOutcome outcome,
        CancellationToken cancellationToken
    ) =>
        outcome.Error is { } error
            ? _workflowStore.RecordStepFailureAsync(
                WorkflowRunId,
                outcome.Sequence,
                error,
                cancellationToken
            )
            : _workflowStore.RecordStepSuccessAsync(
                WorkflowRunId,
                outcome.Sequence,
                outcome.ResultJson,
                cancellationToken
            );

    private void Park()
    {
        // Mirror ctx.Sleep: flag the suspend so the runner fails loudly if user code swallows the
        // exception, then unwind to release the lease.
        ParkRequested = true;
        throw new WorkflowParkException();
    }

    private readonly record struct StepHandle(int Sequence, Guid JobId, WorkflowStep? Step);

    /// <summary>Type-erased resolved outcome of a step, used to record/surface it uniformly.</summary>
    private readonly record struct StepOutcome(
        int Sequence,
        bool Ready,
        string? ResultJson,
        string? Error
    );

    private readonly record struct StepResolution<TOutput>(
        bool Ready,
        TOutput Value,
        string? ResultJson,
        string? Error
    )
    {
        public static StepResolution<TOutput> Pending() => new(false, default!, null, null);

        public static StepResolution<TOutput> Succeeded(TOutput value, string? resultJson) =>
            new(true, value, resultJson, null);

        public static StepResolution<TOutput> Failed(string error) =>
            new(true, default!, null, error);

        public StepOutcome ToOutcome(int sequence) => new(sequence, Ready, ResultJson, Error);
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
