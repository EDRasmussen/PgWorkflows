using Microsoft.Extensions.DependencyInjection;
using Npgsql;
using Xunit;

namespace PgWorkflows.Tests;

public sealed class BuilderTests
{
    [Fact]
    public void UsePostgres_caps_max_pool_size_by_default()
    {
        var connectionString = ResolveConnectionString("Host=localhost;Username=u;Password=p");

        Assert.Equal(
            PgWorkflowsBuilder.DefaultMaxPoolSize,
            new NpgsqlConnectionStringBuilder(connectionString).MaxPoolSize
        );
    }

    [Fact]
    public void UsePostgres_respects_max_pool_size_from_connection_string()
    {
        var connectionString = ResolveConnectionString(
            "Host=localhost;Username=u;Password=p;Maximum Pool Size=64"
        );

        Assert.Equal(64, new NpgsqlConnectionStringBuilder(connectionString).MaxPoolSize);
    }

    [Fact]
    public void UsePostgres_max_pool_size_parameter_overrides_connection_string()
    {
        var connectionString = ResolveConnectionString(
            "Host=localhost;Username=u;Password=p;Maximum Pool Size=64",
            maxPoolSize: 8
        );

        Assert.Equal(8, new NpgsqlConnectionStringBuilder(connectionString).MaxPoolSize);
    }

    private static string ResolveConnectionString(string connectionString, int? maxPoolSize = null)
    {
        var services = new ServiceCollection();
        services.AddPgWorkflows(pg =>
            pg.UsePostgres(connectionString, maxPoolSize: maxPoolSize).DisableWorkers()
        );

        using var provider = services.BuildServiceProvider();
        return provider.GetRequiredService<NpgsqlDataSource>().ConnectionString;
    }
}
