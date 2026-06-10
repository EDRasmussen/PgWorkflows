using System.Reflection;
using System.Runtime.ExceptionServices;
using System.Text.Json;
using Microsoft.Extensions.DependencyInjection;
using Npgsql;
using PgWorkflows.Activities;
using PgWorkflows.Persistence;
using PgWorkflows.Persistence.Postgres;
using PgWorkflows.Workers;
using PgWorkflows.Workflows;

namespace PgWorkflows;

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

    public IServiceCollection Services { get; }

    internal bool EnsurePostgresSchemaOnStart { get; private set; }

    internal bool RunWorkers { get; private set; } = true;

    internal ActivityWorkerOptions ActivityWorkerOptions { get; private set; } = new();

    internal WorkflowWorkerOptions WorkflowWorkerOptions { get; private set; } = new();

    /// <summary>
    /// Per-process connection pool cap applied when neither <c>maxPoolSize</c> nor the connection
    /// string says otherwise. Npgsql's own default of 100 equals Postgres' default
    /// <c>max_connections</c>, so a handful of API and worker processes sharing one database can
    /// exhaust it under load; 20 keeps a typical fleet comfortably below the limit.
    /// </summary>
    internal const int DefaultMaxPoolSize = 20;

    /// <param name="maxPoolSize">
    /// Maximum number of pooled connections this process opens. When null, a
    /// <c>Maximum Pool Size</c> from the connection string is respected, and
    /// <see cref="DefaultMaxPoolSize"/> applies if the connection string does not set one.
    /// </param>
    public PgWorkflowsBuilder UsePostgres(
        string connectionString,
        bool ensureSchemaOnStart = true,
        int? maxPoolSize = null
    )
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(connectionString);

        var connectionStringBuilder = new NpgsqlConnectionStringBuilder(connectionString);
        if (maxPoolSize is { } explicitMaxPoolSize)
        {
            connectionStringBuilder.MaxPoolSize = explicitMaxPoolSize;
        }
        else if (
            !connectionStringBuilder
                .Keys.Cast<string>()
                .Contains("Maximum Pool Size", StringComparer.OrdinalIgnoreCase)
        )
        {
            connectionStringBuilder.MaxPoolSize = DefaultMaxPoolSize;
        }

        var effectiveConnectionString = connectionStringBuilder.ConnectionString;
        Services.AddSingleton(_ => NpgsqlDataSource.Create(effectiveConnectionString));
        AddPostgresStore(ensureSchemaOnStart);
        return this;
    }

    public PgWorkflowsBuilder UsePostgres(
        NpgsqlDataSource dataSource,
        bool ensureSchemaOnStart = true
    )
    {
        ArgumentNullException.ThrowIfNull(dataSource);

        Services.AddSingleton(dataSource);
        AddPostgresStore(ensureSchemaOnStart);
        return this;
    }

    /// <summary>
    /// Makes this registration client-only: the process can start, signal, and await workflows
    /// through <see cref="IPgWorkflowClient"/>, but runs no background workers — started runs are
    /// executed by other processes pointed at the same database. Use this in a front-facing API
    /// that dispatches work to a separately scaled worker fleet.
    /// </summary>
    public PgWorkflowsBuilder DisableWorkers()
    {
        RunWorkers = false;
        return this;
    }

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

    public PgWorkflowsBuilder RegisterActivity<TInput, TOutput>(
        string activityName,
        Func<TInput, TOutput> handler,
        JsonSerializerOptions? jsonSerializerOptions = null
    )
    {
        _registry.Register(activityName, handler, jsonSerializerOptions);
        return this;
    }

    public PgWorkflowsBuilder RegisterActivity<TInput, TOutput>(
        string activityName,
        Func<TInput, ValueTask<TOutput>> handler,
        JsonSerializerOptions? jsonSerializerOptions = null
    )
    {
        _registry.Register(activityName, handler, jsonSerializerOptions);
        return this;
    }

    public PgWorkflowsBuilder RegisterActivity<TInput, TOutput>(
        string activityName,
        Func<TInput, CancellationToken, ValueTask<TOutput>> handler,
        JsonSerializerOptions? jsonSerializerOptions = null
    )
    {
        _registry.Register(activityName, handler, jsonSerializerOptions);
        return this;
    }

    public PgWorkflowsBuilder RegisterActivity<TInput, TOutput>(
        string activityName,
        Func<ActivityExecutionContext, TInput, CancellationToken, ValueTask<TOutput>> handler,
        JsonSerializerOptions? jsonSerializerOptions = null
    )
    {
        _registry.Register(activityName, handler, jsonSerializerOptions);
        return this;
    }

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
        ValidateActivityMethod(method);

        var attribute = method.GetCustomAttribute<ActivityAttribute>()!;
        var name = string.IsNullOrWhiteSpace(attribute.Name) ? method.Name : attribute.Name!;
        return new DiscoveredActivity(name, method);
    }

    private static void ValidateActivityMethod(MethodInfo method)
    {
        var displayName = $"{method.DeclaringType?.FullName}.{method.Name}";

        if (method.IsGenericMethodDefinition)
        {
            throw new InvalidOperationException(
                $"Activity method '{displayName}' must not be generic."
            );
        }

        var parameters = method.GetParameters();
        if (parameters.Any(parameter => parameter.ParameterType.IsByRef))
        {
            throw new InvalidOperationException(
                $"Activity method '{displayName}' must not use ref, out, or in parameters."
            );
        }

        var cancellationTokens = parameters
            .Where(parameter => parameter.ParameterType == typeof(CancellationToken))
            .ToArray();
        if (cancellationTokens.Length > 1)
        {
            throw new InvalidOperationException(
                $"Activity method '{displayName}' must accept at most one CancellationToken."
            );
        }

        if (
            cancellationTokens.Length == 1
            && parameters[^1].ParameterType != typeof(CancellationToken)
        )
        {
            throw new InvalidOperationException(
                $"Activity method '{displayName}' must put CancellationToken last."
            );
        }
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

            object? returned;
            try
            {
                returned = method.Invoke(activity, args);
            }
            catch (TargetInvocationException ex) when (ex.InnerException is not null)
            {
                ExceptionDispatchInfo.Capture(ex.InnerException).Throw();
                throw;
            }

            var result = await AwaitResultAsync(returned, method.ReturnType);
            return HasNoResult(method.ReturnType)
                ? null
                : JsonSerializer.Serialize(result, jsonSerializerOptions);
        };
    }

    private static bool HasNoResult(Type returnType) =>
        returnType == typeof(void) || returnType == typeof(Task) || returnType == typeof(ValueTask);

    private static object? GetDefault(Type type) =>
        type.IsValueType ? Activator.CreateInstance(type) : null;

    private static async ValueTask<object?> AwaitResultAsync(object? returned, Type returnType)
    {
        if (returnType == typeof(void))
        {
            return null;
        }

        if (returnType == typeof(ValueTask))
        {
            await ((ValueTask)returned!).AsTask();
            return null;
        }

        if (
            returnType.IsGenericType
            && returnType.GetGenericTypeDefinition() == typeof(ValueTask<>)
        )
        {
            var task = (Task)
                returnType
                    .GetMethod(nameof(ValueTask.AsTask), Type.EmptyTypes)!
                    .Invoke(returned, null)!;
            await task;
            return task.GetType().GetProperty(nameof(Task<object>.Result))!.GetValue(task);
        }

        if (typeof(Task).IsAssignableFrom(returnType))
        {
            var task = (Task)returned!;
            await task;
            return returnType.IsGenericType
                ? returnType.GetProperty(nameof(Task<object>.Result))!.GetValue(task)
                : null;
        }

        return returned;
    }

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
            provider.GetRequiredService<WorkflowRunner>(),
            provider,
            executeWorkflowsInCaller: false
        ));
    }
}
