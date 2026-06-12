using Npgsql;
using PgWorkflows.Persistence.Postgres;
using Xunit;

namespace PgWorkflows.Tests;

/// <summary>
/// Base class that gives every test its own freshly-created database on the shared
/// container, with the schema already applied. This is what makes the suite isolated
/// and parallel-safe: tests never share rows, so <c>LeaseAsync</c> can't pick up
/// another test's jobs.
/// </summary>
[Collection(PostgresCollection.Name)]
public abstract class PostgresTestBase : IAsyncLifetime
{
    private readonly PostgresFixture _fixture;
    private string _databaseName = default!;
    private NpgsqlDataSource _dataSource = default!;

    protected PostgresTestBase(PostgresFixture fixture) => _fixture = fixture;

    protected NpgsqlDataSource DataSource => _dataSource;

    internal PostgresActivityJobStore Store { get; private set; } = default!;

    internal PostgresWorkflowStore WorkflowStore { get; private set; } = default!;

    public async Task InitializeAsync()
    {
        _databaseName = "pgw_" + Guid.NewGuid().ToString("N");

        await using (var admin = NpgsqlDataSource.Create(_fixture.AdminConnectionString))
        {
            await using var create = admin.CreateCommand($"create database \"{_databaseName}\";");
            await create.ExecuteNonQueryAsync();
        }

        var builder = new NpgsqlConnectionStringBuilder(_fixture.AdminConnectionString)
        {
            Database = _databaseName,
        };
        _dataSource = NpgsqlDataSource.Create(builder.ConnectionString);
        Store = new PostgresActivityJobStore(_dataSource);
        WorkflowStore = new PostgresWorkflowStore(_dataSource);
        await Store.EnsureSchemaAsync();
    }

    public async Task DisposeAsync()
    {
        await _dataSource.DisposeAsync();

        await using var admin = NpgsqlDataSource.Create(_fixture.AdminConnectionString);
        await using var drop = admin.CreateCommand(
            $"drop database if exists \"{_databaseName}\" with (force);"
        );
        await drop.ExecuteNonQueryAsync();
    }

    /// <summary>Awaits a worker run that is expected to unwind via cancellation (e.g. a
    /// stopped or "crashed" worker), swallowing the resulting cancellation.</summary>
    protected static async Task SwallowCancellation(Task task)
    {
        try
        {
            await task;
        }
        catch (OperationCanceledException)
        {
            // expected when a worker is stopped or simulated as crashed
        }
    }
}
