using Microsoft.Extensions.Hosting;
using PgWorkflows.Persistence;
using PgWorkflows.Persistence.Postgres;
using PgWorkflows.Workers;

namespace PgWorkflows;

internal sealed class PgWorkflowsHostedService(
    ActivityWorker? worker,
    IActivityJobStore store,
    WorkflowWorker? workflowWorker,
    bool ensurePostgresSchemaOnStart
) : IHostedService
{
    private readonly ActivityWorker? _worker = worker;
    private readonly IActivityJobStore _store = store ?? throw new ArgumentNullException(nameof(store));
    private readonly WorkflowWorker? _workflowWorker = workflowWorker;
    private readonly bool _ensurePostgresSchemaOnStart = ensurePostgresSchemaOnStart;
    private CancellationTokenSource? _stopping;
    private Task? _activityRunTask;
    private Task? _workflowRunTask;

    public async Task StartAsync(CancellationToken cancellationToken)
    {
        if (_ensurePostgresSchemaOnStart && _store is PostgresActivityJobStore postgresStore)
        {
            await postgresStore.EnsureSchemaAsync(cancellationToken);
        }

        _stopping = new CancellationTokenSource();
        _activityRunTask = _worker?.RunAsync(_stopping.Token);
        _workflowRunTask = _workflowWorker?.RunAsync(_stopping.Token);

        if (_activityRunTask is { IsCompleted: true })
        {
            await _activityRunTask;
        }

        if (_workflowRunTask is { IsCompleted: true })
        {
            await _workflowRunTask;
        }
    }

    public async Task StopAsync(CancellationToken cancellationToken)
    {
        if (_stopping is null)
        {
            return;
        }

        await _stopping.CancelAsync();

        try
        {
            Task[] tasks = [.. new[] { _activityRunTask, _workflowRunTask }.OfType<Task>()];
            if (tasks.Length > 0)
            {
                await Task.WhenAll(tasks).WaitAsync(cancellationToken);
            }
        }
        finally
        {
            _stopping.Dispose();
        }
    }
}
