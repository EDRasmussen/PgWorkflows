using Microsoft.Extensions.Hosting;
using PgWorkflows.Persistence;
using PgWorkflows.Persistence.Postgres;
using PgWorkflows.Workers;

namespace PgWorkflows;

internal sealed class PgWorkflowsHostedService(
    ActivityWorker worker,
    IActivityJobStore store,
    bool ensurePostgresSchemaOnStart
) : IHostedService
{
    private readonly ActivityWorker _worker = worker ?? throw new ArgumentNullException(nameof(worker));
    private readonly IActivityJobStore _store = store ?? throw new ArgumentNullException(nameof(store));
    private readonly bool _ensurePostgresSchemaOnStart = ensurePostgresSchemaOnStart;
    private CancellationTokenSource? _stopping;
    private Task? _runTask;

    public async Task StartAsync(CancellationToken cancellationToken)
    {
        if (_ensurePostgresSchemaOnStart && _store is PostgresActivityJobStore postgresStore)
        {
            await postgresStore.EnsureSchemaAsync(cancellationToken);
        }

        _stopping = new CancellationTokenSource();
        _runTask = _worker.RunAsync(_stopping.Token);

        if (_runTask.IsCompleted)
        {
            await _runTask;
        }
    }

    public async Task StopAsync(CancellationToken cancellationToken)
    {
        if (_runTask is null || _stopping is null)
        {
            return;
        }

        await _stopping.CancelAsync();

        try
        {
            await _runTask.WaitAsync(cancellationToken);
        }
        finally
        {
            _stopping.Dispose();
        }
    }
}
