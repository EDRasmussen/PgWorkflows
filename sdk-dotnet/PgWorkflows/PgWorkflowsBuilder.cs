using System.Reflection;
using System.Text.Json;
using Microsoft.Extensions.DependencyInjection;
using Npgsql;
using PgWorkflows.Activities;
using PgWorkflows.Internal;
using PgWorkflows.Persistence;
using PgWorkflows.Persistence.Postgres;
using PgWorkflows.Workers;
using PgWorkflows.Workflows;

namespace PgWorkflows;

/// <summary>
/// Fluent configuration surface for <c>AddPgWorkflows(pg =&gt; ...)</c>: point PgWorkflows at a
/// database with <see cref="UsePostgres(string, bool, int?)"/>, register workflows and
/// activities, and tune the hosted workers.
/// </summary>
public sealed class PgWorkflowsBuilder
{
    private readonly ActivityRegistry _registry;
    private readonly WorkflowRegistry _workflowRegistry;
    private readonly List<Action<IServiceProvider>> _deferredRegistrations = [];

    internal PgWorkflowsBuilder(
        IServiceCollection services,
        ActivityRegistry registry,
        WorkflowRegistry workflowRegistry
    )
    {
        Services = services ?? throw new ArgumentNullException(nameof(services));
        _registry = registry ?? throw new ArgumentNullException(nameof(registry));
        _workflowRegistry =
            workflowRegistry ?? throw new ArgumentNullException(nameof(workflowRegistry));
    }

    /// <summary>The service collection being configured, for advanced composition.</summary>
    public IServiceCollection Services { get; }

    internal bool EnsurePostgresSchemaOnStart { get; private set; }

    internal bool RunWorkers { get; private set; } = true;

    internal ActivityWorkerOptions ActivityWorkerOptions { get; private set; } = new();

    internal WorkflowWorkerOptions WorkflowWorkerOptions { get; private set; } = new();

    internal int? EffectiveMaxPoolSize { get; private set; }

    private string? _postgresConnectionString;
    private int? _explicitMaxPoolSize;
    private bool _poolExplicit;
    private string? _resolvedConnectionString;

    private string ResolvedConnectionString =>
        _resolvedConnectionString
        ?? throw new InvalidOperationException(
            "UsePostgres connection string was not finalized before the data source was resolved."
        );

    /// <summary>
    /// Default connection-acquisition timeout. Npgsql's own default is 15s; under a pool that's
    /// momentarily exhausted that turns every store call into a 15s stall. A few seconds lets the
    /// worker's backoff handle a transient pool-full gracefully instead of freezing.
    /// </summary>
    internal const int DefaultConnectionTimeoutSeconds = 5;

    /// <summary>
    /// Spare connections beyond the worker concurrency totals: the two lease-heartbeat loops plus
    /// each worker's lease/poll connection.
    /// </summary>
    internal const int ConnectionHeadroom = 4;

    /// <summary>
    /// Per-process connection pool cap applied when neither <c>maxPoolSize</c> nor the connection
    /// string says otherwise. Npgsql's own default of 100 equals Postgres' default
    /// <c>max_connections</c>, so a handful of API and worker processes sharing one database can
    /// exhaust it under load; 20 keeps a typical fleet comfortably below the limit.
    /// </summary>
    internal const int DefaultMaxPoolSize = 20;

    /// <summary>
    /// Points PgWorkflows at your Postgres database. Creates the schema on startup by default and
    /// sizes the connection pool to the configured worker concurrency unless overridden.
    /// </summary>
    /// <param name="connectionString">The Npgsql connection string.</param>
    /// <param name="ensureSchemaOnStart">Apply the PgWorkflows schema idempotently at startup.</param>
    /// <param name="maxPoolSize">
    /// Maximum number of pooled connections this process opens. When null, the pool is sized to fit
    /// the worker concurrency (activity + workflow + headroom), or a small default for a
    /// client-only process. A <c>Maximum Pool Size</c> in the connection string also counts as an
    /// explicit override.
    /// </param>
    public PgWorkflowsBuilder UsePostgres(
        string connectionString,
        bool ensureSchemaOnStart = true,
        int? maxPoolSize = null
    )
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(connectionString);

        _postgresConnectionString = connectionString;
        _explicitMaxPoolSize = maxPoolSize;
        _poolExplicit =
            maxPoolSize.HasValue
            || new NpgsqlConnectionStringBuilder(connectionString)
                .Keys.Cast<string>()
                .Contains("Maximum Pool Size", StringComparer.OrdinalIgnoreCase);

        Services.AddSingleton(_ => NpgsqlDataSource.Create(ResolvedConnectionString));
        AddPostgresStore(ensureSchemaOnStart);
        return this;
    }

    /// <summary>
    /// Points PgWorkflows at a pre-built <see cref="NpgsqlDataSource"/>. The data source owns its
    /// own pool, which is validated against the configured worker concurrency (failing fast if
    /// too small) but never resized.
    /// </summary>
    public PgWorkflowsBuilder UsePostgres(
        NpgsqlDataSource dataSource,
        bool ensureSchemaOnStart = true
    )
    {
        ArgumentNullException.ThrowIfNull(dataSource);

        // A pre-built data source owns its own pool; we can only validate it, not size it.
        _poolExplicit = true;
        if (
            new NpgsqlConnectionStringBuilder(dataSource.ConnectionString)
                is { MaxPoolSize: var pool }
            && pool > 0
        )
        {
            EffectiveMaxPoolSize = pool;
        }

        Services.AddSingleton(dataSource);
        AddPostgresStore(ensureSchemaOnStart);
        return this;
    }

    /// <summary>
    /// Finalizes the connection pool now that worker concurrency is known. When the pool was not set
    /// explicitly it is sized to the concurrency the workers need, so the two stay coupled by
    /// construction; there is nothing to reconcile. An explicit pool that is too small fails fast.
    /// </summary>
    internal void FinalizeConnectionPool()
    {
        var requiredForWorkers =
            Math.Max(ActivityWorkerOptions.MaxConcurrency, 1)
            + Math.Max(WorkflowWorkerOptions.MaxConcurrency, 1)
            + ConnectionHeadroom;

        if (_postgresConnectionString is { } connectionString)
        {
            var csb = new NpgsqlConnectionStringBuilder(connectionString);
            if (_explicitMaxPoolSize is { } explicitMaxPoolSize)
            {
                csb.MaxPoolSize = explicitMaxPoolSize;
            }
            else if (!_poolExplicit)
            {
                csb.MaxPoolSize = RunWorkers ? requiredForWorkers : DefaultMaxPoolSize;
            }

            if (!csb.Keys.Cast<string>().Contains("Timeout", StringComparer.OrdinalIgnoreCase))
            {
                csb.Timeout = DefaultConnectionTimeoutSeconds;
            }

            EffectiveMaxPoolSize = csb.MaxPoolSize;
            _resolvedConnectionString = csb.ConnectionString;
        }

        if (_poolExplicit)
        {
            GuardExplicitPool(requiredForWorkers);
        }
    }

    private void GuardExplicitPool(int requiredForWorkers)
    {
        if (!RunWorkers || EffectiveMaxPoolSize is not { } pool || pool >= requiredForWorkers)
        {
            return;
        }

        throw new InvalidOperationException(
            $"PgWorkflows worker concurrency (activity {ActivityWorkerOptions.MaxConcurrency} + "
                + $"workflow {WorkflowWorkerOptions.MaxConcurrency}) needs about {requiredForWorkers} "
                + $"pooled connections, but the pool is capped at {pool} (Maximum Pool Size); workers "
                + $"would stall under load. Raise the pool to >= {requiredForWorkers} (keeping the total "
                + $"across all processes under the server's max_connections), or lower MaxConcurrency. "
                + $"Use DisableWorkers() for client-only processes."
        );
    }

    /// <summary>
    /// Makes this registration client-only: the process can start, signal, and await workflows
    /// through <see cref="IPgWorkflowClient"/>, but runs no background workers; started runs are
    /// executed by other processes pointed at the same database. Use this in a front-facing API
    /// that dispatches work to a separately scaled worker fleet.
    /// </summary>
    public PgWorkflowsBuilder DisableWorkers()
    {
        RunWorkers = false;
        return this;
    }

    /// <summary>Tunes the hosted activity worker, e.g. <c>options =&gt; options with { MaxConcurrency = 32 }</c>.</summary>
    public PgWorkflowsBuilder ConfigureActivityWorker(
        Func<ActivityWorkerOptions, ActivityWorkerOptions> configure
    )
    {
        ArgumentNullException.ThrowIfNull(configure);
        ActivityWorkerOptions =
            configure(ActivityWorkerOptions)
            ?? throw new InvalidOperationException(
                "Activity worker options configuration must return an ActivityWorkerOptions instance."
            );
        return this;
    }

    /// <summary>Tunes the hosted workflow worker, e.g. <c>options =&gt; options with { MaxAttempts = 3 }</c>.</summary>
    public PgWorkflowsBuilder ConfigureWorkflowWorker(
        Func<WorkflowWorkerOptions, WorkflowWorkerOptions> configure
    )
    {
        ArgumentNullException.ThrowIfNull(configure);
        WorkflowWorkerOptions =
            configure(WorkflowWorkerOptions)
            ?? throw new InvalidOperationException(
                "Workflow worker options configuration must return a WorkflowWorkerOptions instance."
            );
        return this;
    }

    /// <summary>
    /// Registers a single delegate-based activity under an explicit durable name, a low-level
    /// escape hatch next to <see cref="AddActivities{TActivities}"/>. Input and output are
    /// JSON-serialized at the boundary.
    /// </summary>
    public PgWorkflowsBuilder RegisterActivity<TInput, TOutput>(
        string activityName,
        Func<TInput, TOutput> handler,
        JsonSerializerOptions? jsonSerializerOptions = null
    )
    {
        _registry.Register(activityName, handler, jsonSerializerOptions);
        return this;
    }

    /// <inheritdoc cref="RegisterActivity{TInput, TOutput}(string, Func{TInput, TOutput}, JsonSerializerOptions?)"/>
    public PgWorkflowsBuilder RegisterActivity<TInput, TOutput>(
        string activityName,
        Func<TInput, ValueTask<TOutput>> handler,
        JsonSerializerOptions? jsonSerializerOptions = null
    )
    {
        _registry.Register(activityName, handler, jsonSerializerOptions);
        return this;
    }

    /// <inheritdoc cref="RegisterActivity{TInput, TOutput}(string, Func{TInput, TOutput}, JsonSerializerOptions?)"/>
    public PgWorkflowsBuilder RegisterActivity<TInput, TOutput>(
        string activityName,
        Func<TInput, CancellationToken, ValueTask<TOutput>> handler,
        JsonSerializerOptions? jsonSerializerOptions = null
    )
    {
        _registry.Register(activityName, handler, jsonSerializerOptions);
        return this;
    }

    /// <inheritdoc cref="RegisterActivity{TInput, TOutput}(string, Func{TInput, TOutput}, JsonSerializerOptions?)"/>
    public PgWorkflowsBuilder RegisterActivity<TInput, TOutput>(
        string activityName,
        Func<ActivityExecutionContext, TInput, CancellationToken, ValueTask<TOutput>> handler,
        JsonSerializerOptions? jsonSerializerOptions = null
    )
    {
        _registry.Register(activityName, handler, jsonSerializerOptions);
        return this;
    }

    /// <summary>
    /// Registers every public <c>[Activity]</c> method on <typeparamref name="TActivities"/>.
    /// Instances are resolved from DI per execution; methods can be sync, Task, or ValueTask,
    /// with multiple parameters and an optional trailing CancellationToken.
    /// </summary>
    public PgWorkflowsBuilder AddActivities<TActivities>(
        JsonSerializerOptions? jsonSerializerOptions = null
    )
        where TActivities : class
    {
        _deferredRegistrations.Add(provider =>
            RegisterAttributedActivities<TActivities>(provider, jsonSerializerOptions)
        );
        return this;
    }

    /// <summary>
    /// Registers a <c>[Workflow]</c> class so it can be started through
    /// <see cref="IPgWorkflowClient"/> and executed by the workflow worker.
    /// </summary>
    public PgWorkflowsBuilder AddWorkflow<TWorkflow>()
        where TWorkflow : class
    {
        _workflowRegistry.Register<TWorkflow>();
        return this;
    }

    internal void ApplyDeferredRegistrations(IServiceProvider provider)
    {
        foreach (var registration in _deferredRegistrations)
        {
            registration(provider);
        }
    }

    private void RegisterAttributedActivities<TActivities>(
        IServiceProvider provider,
        JsonSerializerOptions? jsonSerializerOptions
    )
        where TActivities : class
    {
        var activityType = typeof(TActivities);
        var activities = activityType
            .GetMethods(BindingFlags.Instance | BindingFlags.Public)
            .Where(method => method.GetCustomAttribute<ActivityAttribute>() is not null)
            .Select(CreateDiscoveredActivity)
            .ToArray();

        if (activities.Length == 0)
        {
            throw new InvalidOperationException(
                $"Activity type '{activityType.FullName}' has no public methods marked with [Activity]."
            );
        }

        var duplicate = activities
            .GroupBy(activity => activity.Name, StringComparer.Ordinal)
            .FirstOrDefault(group => group.Count() > 1);
        if (duplicate is not null)
        {
            throw new InvalidOperationException(
                $"Activity type '{activityType.FullName}' defines multiple activities named '{duplicate.Key}'. Activity names must be unique."
            );
        }

        foreach (var activity in activities)
        {
            if (_registry.TryResolve(activity.Name, out _))
            {
                throw new InvalidOperationException(
                    $"An activity named '{activity.Name}' is already registered."
                );
            }
        }

        foreach (var activity in activities)
        {
            _registry.Register(
                activity.Name,
                CreateHandler<TActivities>(provider, activity.Method, jsonSerializerOptions)
            );
        }
    }

    private static DiscoveredActivity CreateDiscoveredActivity(MethodInfo method)
    {
        ReflectionInvoke.ValidateInvokableMethod(method, "Activity method");

        var attribute = method.GetCustomAttribute<ActivityAttribute>()!;
        var name = string.IsNullOrWhiteSpace(attribute.Name) ? method.Name : attribute.Name!;
        return new DiscoveredActivity(name, method);
    }

    private static ActivityHandler CreateHandler<TActivities>(
        IServiceProvider provider,
        MethodInfo method,
        JsonSerializerOptions? jsonSerializerOptions
    )
        where TActivities : class
    {
        var parameters = method.GetParameters();
        var hasCancellationToken =
            parameters.LastOrDefault()?.ParameterType == typeof(CancellationToken);
        var inputParameterCount = hasCancellationToken ? parameters.Length - 1 : parameters.Length;

        return async (_, inputJson, cancellationToken) =>
        {
            var activity = ActivatorUtilities.GetServiceOrCreateInstance<TActivities>(provider);
            var args = new object?[parameters.Length];

            if (inputParameterCount == 1)
            {
                var inputType = parameters[0].ParameterType;
                args[0] = inputJson is null
                    ? GetDefault(inputType)
                    : JsonSerializer.Deserialize(inputJson, inputType, jsonSerializerOptions);
            }
            else if (inputParameterCount > 1)
            {
                using var document = inputJson is null ? null : JsonDocument.Parse(inputJson);
                for (var index = 0; index < inputParameterCount; index++)
                {
                    var parameter = parameters[index];
                    args[index] =
                        document is not null
                        && document.RootElement.TryGetProperty(parameter.Name!, out var property)
                            ? property.Deserialize(parameter.ParameterType, jsonSerializerOptions)
                            : GetDefault(parameter.ParameterType);
                }
            }

            if (hasCancellationToken)
            {
                args[^1] = cancellationToken;
            }

            var returned = ReflectionInvoke.InvokeUnwrapped(method, activity, args);
            var result = await ReflectionInvoke.AwaitResultAsync(returned, method.ReturnType);
            return HasNoResult(method.ReturnType)
                ? null
                : JsonSerializer.Serialize(result, jsonSerializerOptions);
        };
    }

    private static bool HasNoResult(Type returnType) =>
        returnType == typeof(void) || returnType == typeof(Task) || returnType == typeof(ValueTask);

    private static object? GetDefault(Type type) =>
        type.IsValueType ? Activator.CreateInstance(type) : null;

    private sealed record DiscoveredActivity(string Name, MethodInfo Method);

    private void AddPostgresStore(bool ensureSchemaOnStart)
    {
        EnsurePostgresSchemaOnStart = ensureSchemaOnStart;
        Services.AddSingleton<PostgresActivityJobStore>();
        Services.AddSingleton<IActivityJobStore>(provider =>
            provider.GetRequiredService<PostgresActivityJobStore>()
        );
        Services.AddSingleton<PostgresWorkflowStore>();
        Services.AddSingleton<IWorkflowStore>(provider =>
            provider.GetRequiredService<PostgresWorkflowStore>()
        );
        Services.AddSingleton(provider => new WorkflowRunner(
            provider.GetRequiredService<IWorkflowStore>(),
            provider.GetRequiredService<IActivityJobStore>(),
            provider.GetService<JsonSerializerOptions>()
        )
        {
            ParkGrace = provider.GetRequiredService<WorkflowWorkerOptions>().ParkGrace,
        });
        Services.AddSingleton<WorkflowWorker>();
        Services.AddSingleton<IPgWorkflowClient>(provider => new PgWorkflowClient(
            provider.GetRequiredService<WorkflowRegistry>(),
            provider.GetRequiredService<WorkflowRunner>()
        ));
    }
}
