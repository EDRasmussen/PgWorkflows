using System.Collections.Concurrent;
using System.Text.Json;
using System.Text.Json.Serialization.Metadata;

namespace PgWorkflows.Activities;

public sealed class ActivityRegistry
{
    private readonly ConcurrentDictionary<string, ActivityHandler> _handlers = new(
        StringComparer.Ordinal
    );

    public void Register(string activityName, ActivityHandler handler)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(activityName);
        ArgumentNullException.ThrowIfNull(handler);

        if (!_handlers.TryAdd(activityName, handler))
        {
            throw new InvalidOperationException(
                $"An activity named '{activityName}' is already registered."
            );
        }
    }

    public void Register<TInput, TOutput>(
        string activityName,
        Func<TInput, TOutput> handler,
        JsonSerializerOptions? jsonSerializerOptions = null
    )
    {
        ArgumentNullException.ThrowIfNull(handler);

        Register<TInput, TOutput>(
            activityName,
            (input, _) => ValueTask.FromResult(handler(input)),
            jsonSerializerOptions
        );
    }

    public void Register<TInput, TOutput>(
        string activityName,
        Func<TInput, ValueTask<TOutput>> handler,
        JsonSerializerOptions? jsonSerializerOptions = null
    )
    {
        ArgumentNullException.ThrowIfNull(handler);

        Register<TInput, TOutput>(
            activityName,
            (input, _) => handler(input),
            jsonSerializerOptions
        );
    }

    public void Register<TInput, TOutput>(
        string activityName,
        Func<TInput, CancellationToken, ValueTask<TOutput>> handler,
        JsonSerializerOptions? jsonSerializerOptions = null
    )
    {
        ArgumentNullException.ThrowIfNull(handler);

        Register<TInput, TOutput>(
            activityName,
            (_, input, cancellationToken) => handler(input, cancellationToken),
            jsonSerializerOptions
        );
    }

    public void Register<TInput, TOutput>(
        string activityName,
        Func<ActivityExecutionContext, TInput, CancellationToken, ValueTask<TOutput>> handler,
        JsonSerializerOptions? jsonSerializerOptions = null
    )
    {
        ArgumentNullException.ThrowIfNull(handler);

        Register(
            activityName,
            async (context, input, cancellationToken) =>
            {
                var typedInput = input is null
                    ? default
                    : JsonSerializer.Deserialize<TInput>(input, jsonSerializerOptions);
                var result = await handler(context, typedInput!, cancellationToken);
                return JsonSerializer.Serialize(result, jsonSerializerOptions);
            }
        );
    }

    public void Register<TInput, TOutput>(
        string activityName,
        Func<ActivityExecutionContext, TInput, CancellationToken, ValueTask<TOutput>> handler,
        JsonTypeInfo<TInput> inputJsonTypeInfo,
        JsonTypeInfo<TOutput> outputJsonTypeInfo
    )
    {
        ArgumentNullException.ThrowIfNull(handler);
        ArgumentNullException.ThrowIfNull(inputJsonTypeInfo);
        ArgumentNullException.ThrowIfNull(outputJsonTypeInfo);

        Register(
            activityName,
            async (context, input, cancellationToken) =>
            {
                var typedInput = input is null
                    ? default
                    : JsonSerializer.Deserialize(input, inputJsonTypeInfo);
                var result = await handler(context, typedInput!, cancellationToken);
                return JsonSerializer.Serialize(result, outputJsonTypeInfo);
            }
        );
    }

    public void RegisterActivity<TActivities, TInput, TOutput>(
        string activityName,
        TActivities activities,
        Func<TActivities, Func<TInput, TOutput>> activityMethod,
        JsonSerializerOptions? jsonSerializerOptions = null
    ) =>
        RegisterActivity(
            activityName,
            () => activities,
            activityMethod,
            jsonSerializerOptions
        );

    public void RegisterActivity<TActivities, TInput, TOutput>(
        string activityName,
        Func<TActivities> activityFactory,
        Func<TActivities, Func<TInput, TOutput>> activityMethod,
        JsonSerializerOptions? jsonSerializerOptions = null
    )
    {
        ArgumentNullException.ThrowIfNull(activityFactory);
        ArgumentNullException.ThrowIfNull(activityMethod);

        Register<TInput, TOutput>(
            activityName,
            input => activityMethod(activityFactory())(input),
            jsonSerializerOptions
        );
    }

    public void RegisterActivity<TActivities, TInput, TOutput>(
        string activityName,
        TActivities activities,
        Func<TActivities, Func<TInput, ValueTask<TOutput>>> activityMethod,
        JsonSerializerOptions? jsonSerializerOptions = null
    ) =>
        RegisterActivity(
            activityName,
            () => activities,
            activityMethod,
            jsonSerializerOptions
        );

    public void RegisterActivity<TActivities, TInput, TOutput>(
        string activityName,
        Func<TActivities> activityFactory,
        Func<TActivities, Func<TInput, ValueTask<TOutput>>> activityMethod,
        JsonSerializerOptions? jsonSerializerOptions = null
    )
    {
        ArgumentNullException.ThrowIfNull(activityFactory);
        ArgumentNullException.ThrowIfNull(activityMethod);

        Register<TInput, TOutput>(
            activityName,
            input => activityMethod(activityFactory())(input),
            jsonSerializerOptions
        );
    }

    public void RegisterActivity<TActivities, TInput, TOutput>(
        string activityName,
        TActivities activities,
        Func<TActivities, Func<TInput, CancellationToken, ValueTask<TOutput>>> activityMethod,
        JsonSerializerOptions? jsonSerializerOptions = null
    ) =>
        RegisterActivity(
            activityName,
            () => activities,
            activityMethod,
            jsonSerializerOptions
        );

    public void RegisterActivity<TActivities, TInput, TOutput>(
        string activityName,
        Func<TActivities> activityFactory,
        Func<TActivities, Func<TInput, CancellationToken, ValueTask<TOutput>>> activityMethod,
        JsonSerializerOptions? jsonSerializerOptions = null
    )
    {
        ArgumentNullException.ThrowIfNull(activityFactory);
        ArgumentNullException.ThrowIfNull(activityMethod);

        Register<TInput, TOutput>(
            activityName,
            (input, cancellationToken) => activityMethod(activityFactory())(input, cancellationToken),
            jsonSerializerOptions
        );
    }

    public bool TryResolve(string activityName, out ActivityHandler? handler)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(activityName);
        return _handlers.TryGetValue(activityName, out handler);
    }
}
