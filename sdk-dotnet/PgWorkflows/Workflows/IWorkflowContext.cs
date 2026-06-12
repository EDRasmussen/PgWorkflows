using System.Linq.Expressions;

namespace PgWorkflows.Workflows;

/// <summary>
/// The durable toolbox passed to a <c>[WorkflowRun]</c> method. Every call on this context is a
/// durable step keyed by its position in the run: results are persisted, and on resume an
/// already-completed step returns its stored result instead of re-running, so side effects happen
/// exactly once. The only rule for workflow code is to call these methods in a stable order.
/// </summary>
public interface IWorkflowContext
{
    /// <summary>The executing run's id in <c>pw_workflow_runs</c>.</summary>
    Guid WorkflowRunId { get; }

    /// <summary>
    /// Durably sleeps for <paramref name="duration"/>. The run is parked (its <c>visible_at</c> is
    /// pushed into the future and the lease released) and resumed by the workflow worker once the
    /// timer fires, surviving process restarts. The deadline is persisted on first encounter so it
    /// stays stable across replays.
    /// </summary>
    /// <remarks>
    /// Parking is implemented by throwing an internal control-flow exception, so do not wrap
    /// <c>Sleep</c> in a broad <c>catch</c> — doing so swallows the park. If that happens the run
    /// fails loudly rather than silently skipping the timer.
    /// </remarks>
    ValueTask Sleep(TimeSpan duration, CancellationToken cancellationToken = default);

    /// <summary>
    /// Waits for one persisted external signal with the given <paramref name="name"/> and returns
    /// its payload. Signals are buffered if delivered before this wait is reached, and multiple
    /// signals with the same name are consumed in delivery order.
    /// </summary>
    /// <remarks>
    /// Like <see cref="Sleep"/>, waiting parks the run by throwing an internal control-flow
    /// exception; do not wrap it in a broad <c>catch</c> that swallows the park.
    /// </remarks>
    ValueTask<TSignal> WaitForSignal<TSignal>(
        string name,
        CancellationToken cancellationToken = default
    );

    /// <summary>
    /// Creates a pending activity for fan-out composition with <c>WhenAll</c>, without awaiting
    /// it. The lambda must be a direct method call on the activity class, e.g.
    /// <c>ctx.CallActivity((MyActivities a) =&gt; a.DoWork(input))</c> — that is how PgWorkflows
    /// knows which activity to enqueue and with which arguments.
    /// </summary>
    WorkflowActivity<TOutput> CallActivity<TActivities, TOutput>(
        Expression<Func<TActivities, TOutput>> activityCall
    );

    /// <inheritdoc cref="CallActivity{TActivities, TOutput}(Expression{Func{TActivities, TOutput}})"/>
    WorkflowActivity<TOutput> CallActivity<TActivities, TOutput>(
        Expression<Func<TActivities, Task<TOutput>>> activityCall
    );

    /// <inheritdoc cref="CallActivity{TActivities, TOutput}(Expression{Func{TActivities, TOutput}})"/>
    WorkflowActivity<TOutput> CallActivity<TActivities, TOutput>(
        Expression<Func<TActivities, ValueTask<TOutput>>> activityCall
    );

    /// <summary>
    /// Runs one activity as a durable step and returns its result. The activity is enqueued to
    /// the job queue and executed by an activity worker; the run parks (releasing its lease)
    /// until the result lands, and the step's outcome is memoized across replays. The lambda must
    /// be a direct method call, e.g. <c>ctx.Activity((MyActivities a) =&gt; a.DoWork(input), ct)</c>.
    /// </summary>
    ValueTask<TOutput> Activity<TActivities, TOutput>(
        Expression<Func<TActivities, TOutput>> activityCall,
        CancellationToken cancellationToken = default
    );

    /// <inheritdoc cref="Activity{TActivities, TOutput}(Expression{Func{TActivities, TOutput}}, CancellationToken)"/>
    ValueTask<TOutput> Activity<TActivities, TOutput>(
        Expression<Func<TActivities, Task<TOutput>>> activityCall,
        CancellationToken cancellationToken = default
    );

    /// <inheritdoc cref="Activity{TActivities, TOutput}(Expression{Func{TActivities, TOutput}}, CancellationToken)"/>
    ValueTask<TOutput> Activity<TActivities, TOutput>(
        Expression<Func<TActivities, ValueTask<TOutput>>> activityCall,
        CancellationToken cancellationToken = default
    );

    /// <summary>
    /// Registers a saga-style compensation activity that runs if the workflow later fails
    /// terminally (after exhausting its attempts). Hooks run in reverse registration order, are
    /// memoized like steps, and capture their arguments at registration time.
    /// </summary>
    ValueTask OnFailure<TActivities>(
        Expression<Action<TActivities>> activityCall,
        CancellationToken cancellationToken = default
    );

    /// <inheritdoc cref="OnFailure{TActivities}(Expression{Action{TActivities}}, CancellationToken)"/>
    ValueTask OnFailure<TActivities>(
        Expression<Func<TActivities, Task>> activityCall,
        CancellationToken cancellationToken = default
    );

    /// <inheritdoc cref="OnFailure{TActivities}(Expression{Action{TActivities}}, CancellationToken)"/>
    ValueTask OnFailure<TActivities>(
        Expression<Func<TActivities, ValueTask>> activityCall,
        CancellationToken cancellationToken = default
    );

    /// <inheritdoc cref="OnFailure{TActivities}(Expression{Action{TActivities}}, CancellationToken)"/>
    ValueTask OnFailure<TActivities, TOutput>(
        Expression<Func<TActivities, TOutput>> activityCall,
        CancellationToken cancellationToken = default
    );

    /// <inheritdoc cref="OnFailure{TActivities}(Expression{Action{TActivities}}, CancellationToken)"/>
    ValueTask OnFailure<TActivities, TOutput>(
        Expression<Func<TActivities, Task<TOutput>>> activityCall,
        CancellationToken cancellationToken = default
    );

    /// <inheritdoc cref="OnFailure{TActivities}(Expression{Action{TActivities}}, CancellationToken)"/>
    ValueTask OnFailure<TActivities, TOutput>(
        Expression<Func<TActivities, ValueTask<TOutput>>> activityCall,
        CancellationToken cancellationToken = default
    );

    /// <summary>
    /// Runs a fan-out of pending activities created by <c>CallActivity</c> as durable steps and
    /// returns their results in order. All siblings are enqueued together so activity workers can
    /// run them concurrently; the run parks until every sibling completes, matching
    /// <see cref="Task.WhenAll(Task[])"/> semantics (the lowest-sequence failure surfaces after
    /// all siblings finish). Survives crashes mid-fan-out.
    /// </summary>
    ValueTask<TOutput[]> WhenAll<TOutput>(
        IEnumerable<WorkflowActivity<TOutput>> activities,
        CancellationToken cancellationToken = default
    );

    /// <inheritdoc cref="WhenAll{TOutput}(IEnumerable{WorkflowActivity{TOutput}}, CancellationToken)"/>
    ValueTask<(T1 First, T2 Second)> WhenAll<T1, T2>(
        WorkflowActivity<T1> first,
        WorkflowActivity<T2> second,
        CancellationToken cancellationToken = default
    );

    /// <inheritdoc cref="WhenAll{TOutput}(IEnumerable{WorkflowActivity{TOutput}}, CancellationToken)"/>
    ValueTask<(T1 First, T2 Second, T3 Third)> WhenAll<T1, T2, T3>(
        WorkflowActivity<T1> first,
        WorkflowActivity<T2> second,
        WorkflowActivity<T3> third,
        CancellationToken cancellationToken = default
    );
}
