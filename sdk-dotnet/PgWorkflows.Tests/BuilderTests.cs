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

    [Fact]
    public void AddPgWorkflows_throws_when_worker_concurrency_exceeds_the_pool()
    {
        var services = new ServiceCollection();

        var ex = Assert.Throws<InvalidOperationException>(() =>
            services.AddPgWorkflows(pg =>
                pg.UsePostgres("Host=localhost;Username=u;Password=p", maxPoolSize: 10)
                    .ConfigureActivityWorker(options => options with { MaxConcurrency = 32 })
                    .ConfigureWorkflowWorker(options => options with { MaxConcurrency = 32 })
            )
        );

        Assert.Contains("pooled connections", ex.Message);
        Assert.Contains("capped at 10", ex.Message);
    }

    [Fact]
    public void UsePostgres_sizes_the_pool_to_worker_concurrency_by_default()
    {
        var connectionString = WorkerConnectionString(activityConcurrency: 15, workflowConcurrency: 15);

        // 15 + 15 + ConnectionHeadroom — the pool follows concurrency with no explicit setting.
        Assert.Equal(
            30 + PgWorkflowsBuilder.ConnectionHeadroom,
            new NpgsqlConnectionStringBuilder(connectionString).MaxPoolSize
        );
    }

    private static string WorkerConnectionString(int activityConcurrency, int workflowConcurrency)
    {
        var services = new ServiceCollection();
        services.AddPgWorkflows(pg =>
            pg.UsePostgres("Host=localhost;Username=u;Password=p")
                .ConfigureActivityWorker(options => options with { MaxConcurrency = activityConcurrency })
                .ConfigureWorkflowWorker(options => options with { MaxConcurrency = workflowConcurrency })
        );

        using var provider = services.BuildServiceProvider();
        return provider.GetRequiredService<NpgsqlDataSource>().ConnectionString;
    }

    [Fact]
    public void AddPgWorkflows_skips_the_guard_for_client_only_processes()
    {
        var services = new ServiceCollection();

        services.AddPgWorkflows(pg =>
            pg.UsePostgres("Host=localhost;Username=u;Password=p", maxPoolSize: 1)
                .ConfigureActivityWorker(options => options with { MaxConcurrency = 64 })
                .DisableWorkers()
        );
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
