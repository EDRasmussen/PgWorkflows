namespace PgWorkflows.Activities;

public interface IActivity;

public interface IActivity<TInput, TOutput> : IActivity
{
    ValueTask<TOutput> RunAsync(TInput input, CancellationToken cancellationToken);
}
