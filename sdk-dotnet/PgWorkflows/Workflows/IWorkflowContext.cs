using System.Linq.Expressions;

namespace PgWorkflows.Workflows;

public interface IWorkflowContext
{
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
    /// <para>
    /// Sleeping requires the hosted workflow worker (which <c>AddPgWorkflows</c> configures by
    /// default). It must not be driven by an inline client (a <c>PgWorkflowClient</c> with
    /// <c>executeWorkflowsInCaller: true</c>); calling <c>Sleep</c> on such a run throws
    /// <see cref="NotSupportedException"/>.
    /// </para>
    /// </remarks>
    ValueTask Sleep(TimeSpan duration, CancellationToken cancellationToken = default);

    WorkflowActivity<TOutput> CallActivity<TActivities, TOutput>(
        Expression<Func<TActivities, TOutput>> activityCall
    );

    WorkflowActivity<TOutput> CallActivity<TActivities, TOutput>(
        Expression<Func<TActivities, Task<TOutput>>> activityCall
    );

    WorkflowActivity<TOutput> CallActivity<TActivities, TOutput>(
        Expression<Func<TActivities, ValueTask<TOutput>>> activityCall
    );

    ValueTask<TOutput> Activity<TActivities, TOutput>(
        Expression<Func<TActivities, TOutput>> activityCall,
        CancellationToken cancellationToken = default
    );

    ValueTask<TOutput> Activity<TActivities, TOutput>(
        Expression<Func<TActivities, Task<TOutput>>> activityCall,
        CancellationToken cancellationToken = default
    );

    ValueTask<TOutput> Activity<TActivities, TOutput>(
        Expression<Func<TActivities, ValueTask<TOutput>>> activityCall,
        CancellationToken cancellationToken = default
    );

    ValueTask OnFailure<TActivities>(
        Expression<Action<TActivities>> activityCall,
        CancellationToken cancellationToken = default
    );

    ValueTask OnFailure<TActivities>(
        Expression<Func<TActivities, Task>> activityCall,
        CancellationToken cancellationToken = default
    );

    ValueTask OnFailure<TActivities>(
        Expression<Func<TActivities, ValueTask>> activityCall,
        CancellationToken cancellationToken = default
    );

    ValueTask OnFailure<TActivities, TOutput>(
        Expression<Func<TActivities, TOutput>> activityCall,
        CancellationToken cancellationToken = default
    );

    ValueTask OnFailure<TActivities, TOutput>(
        Expression<Func<TActivities, Task<TOutput>>> activityCall,
        CancellationToken cancellationToken = default
    );

    ValueTask OnFailure<TActivities, TOutput>(
        Expression<Func<TActivities, ValueTask<TOutput>>> activityCall,
        CancellationToken cancellationToken = default
    );

    ValueTask<TOutput[]> WhenAll<TOutput>(
        IEnumerable<WorkflowActivity<TOutput>> activities,
        CancellationToken cancellationToken = default
    );

    ValueTask<(T1 First, T2 Second)> WhenAll<T1, T2>(
        WorkflowActivity<T1> first,
        WorkflowActivity<T2> second,
        CancellationToken cancellationToken = default
    );

    ValueTask<(T1 First, T2 Second, T3 Third)> WhenAll<T1, T2, T3>(
        WorkflowActivity<T1> first,
        WorkflowActivity<T2> second,
        WorkflowActivity<T3> third,
        CancellationToken cancellationToken = default
    );
}
