using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using PgWorkflows.Activities;
using PgWorkflows.Persistence;
using PgWorkflows.Workflows;
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
        var workflowRegistry = new WorkflowRegistry();
        var builder = new PgWorkflowsBuilder(services, registry, workflowRegistry);
        configure?.Invoke(builder);

        services.AddSingleton(registry);
        services.AddSingleton(workflowRegistry);
        services.AddSingleton(builder.ActivityWorkerOptions);
        services.AddSingleton(builder.WorkflowWorkerOptions);
        services.AddSingleton<ActivityWorker>();
        services.AddSingleton<IHostedService>(provider =>
        {
            builder.ApplyDeferredRegistrations(provider);

            return new PgWorkflowsHostedService(
                provider.GetRequiredService<ActivityWorker>(),
                provider.GetRequiredService<IActivityJobStore>(),
                provider.GetService<WorkflowWorker>(),
                builder.EnsurePostgresSchemaOnStart
            );
        });

        return services;
    }
}
