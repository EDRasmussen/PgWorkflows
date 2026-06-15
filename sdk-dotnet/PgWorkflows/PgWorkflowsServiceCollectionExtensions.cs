using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using PgWorkflows.Activities;
using PgWorkflows.Persistence;
using PgWorkflows.Workers;
using PgWorkflows.Workflows;

namespace PgWorkflows;

/// <summary>Service collection registration for PgWorkflows.</summary>
public static class PgWorkflowsServiceCollectionExtensions
{
    /// <summary>
    /// Adds PgWorkflows to the service collection: the <see cref="Workflows.IPgWorkflowClient"/>,
    /// the Postgres-backed stores, and (unless <see cref="PgWorkflowsBuilder.DisableWorkers"/> is
    /// called) hosted activity and workflow workers that start with the application.
    /// </summary>
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
        builder.FinalizeConnectionPool();

        services.AddSingleton(registry);
        services.AddSingleton(workflowRegistry);
        services.AddSingleton(builder.ActivityWorkerOptions);
        services.AddSingleton(builder.WorkflowWorkerOptions);
        if (builder.RunWorkers)
        {
            services.AddSingleton<ActivityWorker>();
        }
        services.AddSingleton<IHostedService>(provider =>
        {
            builder.ApplyDeferredRegistrations(provider);

            return new PgWorkflowsHostedService(
                builder.RunWorkers ? provider.GetRequiredService<ActivityWorker>() : null,
                provider.GetRequiredService<IActivityJobStore>(),
                builder.RunWorkers ? provider.GetService<WorkflowWorker>() : null,
                builder.EnsurePostgresSchemaOnStart
            );
        });

        return services;
    }
}
