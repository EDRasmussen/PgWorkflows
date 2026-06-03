using System.Reflection;
using System.Runtime.ExceptionServices;
using System.Text.Json;
using Microsoft.Extensions.DependencyInjection;
using Npgsql;
using PgWorkflows.Activities;
using PgWorkflows.Persistence;
using PgWorkflows.Persistence.Postgres;
using PgWorkflows.Workers;

namespace PgWorkflows;

public sealed class PgWorkflowsBuilder
{
    private readonly ActivityRegistry _registry;
    private readonly List<Action<IServiceProvider>> _deferredRegistrations = [];

    internal PgWorkflowsBuilder(IServiceCollection services, ActivityRegistry registry)
    {
        Services = services ?? throw new ArgumentNullException(nameof(services));
        _registry = registry ?? throw new ArgumentNullException(nameof(registry));
    }

    public IServiceCollection Services { get; }

    internal bool EnsurePostgresSchemaOnStart { get; private set; }

    internal ActivityWorkerOptions WorkerOptions { get; private set; } = new();

    public PgWorkflowsBuilder UsePostgres(
        string connectionString,
        bool ensureSchemaOnStart = true
    )
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(connectionString);

        Services.AddSingleton(_ => NpgsqlDataSource.Create(connectionString));
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

    public PgWorkflowsBuilder ConfigureWorker(
        Func<ActivityWorkerOptions, ActivityWorkerOptions> configure
    )
    {
        ArgumentNullException.ThrowIfNull(configure);
        WorkerOptions = configure(WorkerOptions) ?? throw new InvalidOperationException(
            "Worker options configuration must return an ActivityWorkerOptions instance."
        );
        return this;
    }

    public PgWorkflowsBuilder Register<TInput, TOutput>(
        string activityName,
        Func<TInput, TOutput> handler,
        JsonSerializerOptions? jsonSerializerOptions = null
    )
    {
        _registry.Register(activityName, handler, jsonSerializerOptions);
        return this;
    }

    public PgWorkflowsBuilder Register<TInput, TOutput>(
        string activityName,
        Func<TInput, ValueTask<TOutput>> handler,
        JsonSerializerOptions? jsonSerializerOptions = null
    )
    {
        _registry.Register(activityName, handler, jsonSerializerOptions);
        return this;
    }

    public PgWorkflowsBuilder Register<TInput, TOutput>(
        string activityName,
        Func<TInput, CancellationToken, ValueTask<TOutput>> handler,
        JsonSerializerOptions? jsonSerializerOptions = null
    )
    {
        _registry.Register(activityName, handler, jsonSerializerOptions);
        return this;
    }

    public PgWorkflowsBuilder Register<TInput, TOutput>(
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

    public PgWorkflowsBuilder RegisterActivity<TActivities, TInput, TOutput>(
        string activityName,
        Func<TActivities, Func<TInput, TOutput>> activityMethod,
        JsonSerializerOptions? jsonSerializerOptions = null
    )
        where TActivities : class
    {
        ArgumentNullException.ThrowIfNull(activityMethod);

        _deferredRegistrations.Add(provider =>
            _registry.RegisterActivity(
                activityName,
                () => ActivatorUtilities.GetServiceOrCreateInstance<TActivities>(provider),
                activityMethod,
                jsonSerializerOptions
            )
        );
        return this;
    }

    public PgWorkflowsBuilder RegisterActivity<TActivities, TInput, TOutput>(
        string activityName,
        Func<TActivities, Func<TInput, ValueTask<TOutput>>> activityMethod,
        JsonSerializerOptions? jsonSerializerOptions = null
    )
        where TActivities : class
    {
        ArgumentNullException.ThrowIfNull(activityMethod);

        _deferredRegistrations.Add(provider =>
            _registry.RegisterActivity(
                activityName,
                () => ActivatorUtilities.GetServiceOrCreateInstance<TActivities>(provider),
                activityMethod,
                jsonSerializerOptions
            )
        );
        return this;
    }

    public PgWorkflowsBuilder RegisterActivity<TActivities, TInput, TOutput>(
        string activityName,
        Func<TActivities, Func<TInput, CancellationToken, ValueTask<TOutput>>> activityMethod,
        JsonSerializerOptions? jsonSerializerOptions = null
    )
        where TActivities : class
    {
        ArgumentNullException.ThrowIfNull(activityMethod);

        _deferredRegistrations.Add(provider =>
            _registry.RegisterActivity(
                activityName,
                () => ActivatorUtilities.GetServiceOrCreateInstance<TActivities>(provider),
                activityMethod,
                jsonSerializerOptions
            )
        );
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
        var methods = activityType
            .GetMethods(BindingFlags.Instance | BindingFlags.Public)
            .Where(method => method.GetCustomAttribute<ActivityAttribute>() is not null)
            .ToArray();

        if (methods.Length == 0)
        {
            throw new InvalidOperationException(
                $"Activity type '{activityType.FullName}' has no public methods marked with [Activity]."
            );
        }

        foreach (var method in methods)
        {
            var attribute = method.GetCustomAttribute<ActivityAttribute>()!;
            var activityName = string.IsNullOrWhiteSpace(attribute.Name)
                ? method.Name
                : attribute.Name!;

            _registry.Register(
                activityName,
                CreateHandler<TActivities>(provider, method, jsonSerializerOptions)
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
        if (method.IsGenericMethodDefinition)
        {
            throw new InvalidOperationException(
                $"Activity method '{method.DeclaringType?.FullName}.{method.Name}' must not be generic."
            );
        }

        var parameters = method.GetParameters();
        if (parameters.Length > 2)
        {
            throw new InvalidOperationException(
                $"Activity method '{method.DeclaringType?.FullName}.{method.Name}' must accept at most one input parameter and an optional CancellationToken."
            );
        }

        var hasCancellationToken = parameters.LastOrDefault()?.ParameterType == typeof(CancellationToken);
        var inputParameterCount = hasCancellationToken ? parameters.Length - 1 : parameters.Length;
        if (inputParameterCount > 1)
        {
            throw new InvalidOperationException(
                $"Activity method '{method.DeclaringType?.FullName}.{method.Name}' must accept at most one input parameter before CancellationToken."
            );
        }

        var inputType = inputParameterCount == 1 ? parameters[0].ParameterType : null;

        return async (_, inputJson, cancellationToken) =>
        {
            var activity = ActivatorUtilities.GetServiceOrCreateInstance<TActivities>(provider);
            var args = new object?[parameters.Length];

            if (inputType is not null)
            {
                args[0] = inputJson is null
                    ? GetDefault(inputType)
                    : JsonSerializer.Deserialize(inputJson, inputType, jsonSerializerOptions);
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

        if (returnType.IsGenericType && returnType.GetGenericTypeDefinition() == typeof(ValueTask<>))
        {
            var task = (Task)returnType.GetMethod(nameof(ValueTask.AsTask), Type.EmptyTypes)!
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

    private void AddPostgresStore(bool ensureSchemaOnStart)
    {
        EnsurePostgresSchemaOnStart = ensureSchemaOnStart;
        Services.AddSingleton<PostgresActivityJobStore>();
        Services.AddSingleton<IActivityJobStore>(provider =>
            provider.GetRequiredService<PostgresActivityJobStore>()
        );
    }
}
