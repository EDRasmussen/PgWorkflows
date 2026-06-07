using System.Linq.Expressions;

namespace PgWorkflows.Workflows;

public interface IWorkflowContext
{
    Guid WorkflowRunId { get; }

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
}
