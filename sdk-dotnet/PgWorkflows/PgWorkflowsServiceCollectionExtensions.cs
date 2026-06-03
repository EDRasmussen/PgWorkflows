using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using PgWorkflows.Activities;
using PgWorkflows.Persistence;
using PgWorkflows.Workers;

namespace PgWorkflows;

public static class PgWorkflowsServiceCollectionExtensions
{
    public static IServiceCollection AddPgWorkflows(
        this IServiceCollection services,
        Action<PgWorkflowsBuilder>? configure = null
    )
    {
        ArgumentNullException.ThrowIfNull(services);

        var registry = new ActivityRegistry();
        var builder = new PgWorkflowsBuilder(services, registry);
        configure?.Invoke(builder);

        services.AddSingleton(registry);
        services.AddSingleton(builder.WorkerOptions);
        services.AddSingleton<ActivityWorker>();
        services.AddSingleton<IHostedService>(provider =>
        {
            builder.ApplyDeferredRegistrations(provider);

            return new PgWorkflowsHostedService(
                provider.GetRequiredService<ActivityWorker>(),
                provider.GetRequiredService<IActivityJobStore>(),
                builder.EnsurePostgresSchemaOnStart
            );
        });

        return services;
    }
}
