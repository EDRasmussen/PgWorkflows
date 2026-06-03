using Npgsql;
using System.Data;
using PgWorkflows.Workers;

namespace PgWorkflows.Persistence.Postgres;

public sealed class PostgresActivityJobWakeup(NpgsqlDataSource dataSource)
    : IActivityJobWakeup,
      IAsyncDisposable
{
    private const string Channel = "pgworkflows_activity_jobs";

    private readonly NpgsqlDataSource _dataSource =
        dataSource ?? throw new ArgumentNullException(nameof(dataSource));
    private readonly SemaphoreSlim _gate = new(1, 1);
    private NpgsqlConnection? _connection;

    public async ValueTask WaitAsync(
        TimeSpan timeout,
        CancellationToken cancellationToken = default
    )
    {
        if (timeout <= TimeSpan.Zero)
        {
            return;
        }

        await _gate.WaitAsync(cancellationToken);
        try
        {
            try
            {
                var connection = await GetConnectionAsync(cancellationToken);
                using var timeoutCts = CancellationTokenSource.CreateLinkedTokenSource(
                    cancellationToken
                );
                timeoutCts.CancelAfter(timeout);

                try
                {
                    await connection.WaitAsync(timeoutCts.Token);
                }
                catch (OperationCanceledException) when (!cancellationToken.IsCancellationRequested)
                {
                    // Timeout: polling remains the correctness fallback.
                }
            }
            catch (OperationCanceledException) when (cancellationToken.IsCancellationRequested)
            {
                throw;
            }
            catch
            {
                await ResetConnectionAsync();
                try
                {
                    await Task.Delay(timeout, cancellationToken);
                }
                catch (OperationCanceledException) when (cancellationToken.IsCancellationRequested)
                {
                    throw;
                }
            }
        }
        finally
        {
            _gate.Release();
        }
    }

    public async ValueTask DisposeAsync()
    {
        await ResetConnectionAsync();
        _gate.Dispose();
    }

    private async ValueTask<NpgsqlConnection> GetConnectionAsync(CancellationToken cancellationToken)
    {
        if (_connection is { State: ConnectionState.Open })
        {
            return _connection;
        }

        await ResetConnectionAsync();
        _connection = await _dataSource.OpenConnectionAsync(cancellationToken);
        await using var command = new NpgsqlCommand($"listen {Channel};", _connection);
        await command.ExecuteNonQueryAsync(cancellationToken);
        return _connection;
    }

    private async ValueTask ResetConnectionAsync()
    {
        if (_connection is null)
        {
            return;
        }

        await _connection.DisposeAsync();
        _connection = null;
    }
}
