using System.Linq.Expressions;

namespace PgWorkflows.Workflows;

public interface IWorkflowContext
{
    Guid WorkflowRunId { get; }

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
