using Npgsql;
using Testcontainers.PostgreSql;
using Xunit;

namespace PgWorkflows.Tests;

/// <summary>
/// Starts a single throwaway Postgres container for the whole test assembly.
/// Each test creates its own fresh database (see <see cref="PostgresTestBase"/>),
/// so tests are fully isolated and safe to run in parallel against one container.
/// </summary>
public sealed class PostgresFixture : IAsyncLifetime
{
    private readonly PostgreSqlContainer _container = new PostgreSqlBuilder()
        .WithImage("postgres:16-alpine")
        .Build();

    public string AdminConnectionString => _container.GetConnectionString();

    public Task InitializeAsync() => _container.StartAsync();

    public Task DisposeAsync() => _container.DisposeAsync().AsTask();
}

[CollectionDefinition(Name)]
public sealed class PostgresCollection : ICollectionFixture<PostgresFixture>
{
    public const string Name = "postgres";
}
