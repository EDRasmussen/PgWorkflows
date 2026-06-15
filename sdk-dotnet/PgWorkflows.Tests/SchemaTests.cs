using PgWorkflows.Persistence.Postgres;
using Xunit;

namespace PgWorkflows.Tests;

public sealed class SchemaTests(PostgresFixture fixture) : PostgresTestBase(fixture)
{
    [Fact]
    public void Migrations_are_sequential_from_one()
    {
        var versions = PostgresSchema.Migrations.Select(m => m.Version).ToArray();
        Assert.Equal(Enumerable.Range(1, versions.Length), versions);
    }

    [Fact]
    public async Task EnsureSchema_records_every_migration_version()
    {
        // PostgresTestBase already ran EnsureSchemaAsync against a fresh database.
        var applied = await AppliedVersionsAsync();
        Assert.Equal(PostgresSchema.Migrations.Select(m => m.Version), applied);
    }

    [Fact]
    public async Task EnsureSchema_is_idempotent()
    {
        await Store.EnsureSchemaAsync();
        await Store.EnsureSchemaAsync();

        var applied = await AppliedVersionsAsync();
        Assert.Equal(PostgresSchema.Migrations.Select(m => m.Version), applied);
    }

    [Fact]
    public async Task EnsureSchema_adopts_database_created_before_versioning()
    {
        // A database from before pw_schema_migrations existed has the tables but no bookkeeping.
        // Dropping the bookkeeping table simulates it; EnsureSchemaAsync must adopt such a
        // database by recording the baseline instead of failing on existing tables.
        await using (var drop = DataSource.CreateCommand("drop table pw_schema_migrations;"))
        {
            await drop.ExecuteNonQueryAsync();
        }

        await Store.EnsureSchemaAsync();

        var applied = await AppliedVersionsAsync();
        Assert.Equal(PostgresSchema.Migrations.Select(m => m.Version), applied);
    }

    private async Task<IReadOnlyList<int>> AppliedVersionsAsync()
    {
        var versions = new List<int>();
        await using var command = DataSource.CreateCommand(
            "select version from pw_schema_migrations order by version;"
        );
        await using var reader = await command.ExecuteReaderAsync();
        while (await reader.ReadAsync())
        {
            versions.Add(reader.GetInt32(0));
        }

        return versions;
    }
}
