using System.Collections.Concurrent;
using System.Reflection;
using System.Runtime.ExceptionServices;
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

    public void Register<TActivity>(
        string? activityName = null,
        JsonSerializerOptions? jsonSerializerOptions = null
    )
        where TActivity : IActivity, new()
    {
        var activityType = typeof(TActivity);
        var contract = GetActivityContract(activityType);
        var registerMethod = GetType()
            .GetMethod(nameof(RegisterActivityType), BindingFlags.Instance | BindingFlags.NonPublic)!
            .MakeGenericMethod(activityType, contract.InputType, contract.OutputType);

        try
        {
            registerMethod.Invoke(
                this,
                [
                    activityName ?? ActivityName.For(activityType),
                    new Func<TActivity>(() => new TActivity()),
                    jsonSerializerOptions,
                ]
            );
        }
        catch (TargetInvocationException ex) when (ex.InnerException is not null)
        {
            ExceptionDispatchInfo.Capture(ex.InnerException).Throw();
        }
    }

    public bool TryResolve(string activityName, out ActivityHandler? handler)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(activityName);
        return _handlers.TryGetValue(activityName, out handler);
    }

    private void RegisterActivityType<TActivity, TInput, TOutput>(
        string activityName,
        Func<TActivity> activityFactory,
        JsonSerializerOptions? jsonSerializerOptions
    )
        where TActivity : IActivity<TInput, TOutput>
    {
        Register<TInput, TOutput>(
            activityName,
            (_, input, cancellationToken) => activityFactory().RunAsync(input!, cancellationToken),
            jsonSerializerOptions
        );
    }

    private static (Type InputType, Type OutputType) GetActivityContract(Type activityType)
    {
        var contracts = activityType
            .GetInterfaces()
            .Where(i => i.IsGenericType && i.GetGenericTypeDefinition() == typeof(IActivity<,>))
            .ToArray();

        return contracts.Length switch
        {
            1 => (contracts[0].GetGenericArguments()[0], contracts[0].GetGenericArguments()[1]),
            0 => throw new InvalidOperationException(
                $"Activity type '{activityType.FullName}' must implement IActivity<TInput, TOutput>."
            ),
            _ => throw new InvalidOperationException(
                $"Activity type '{activityType.FullName}' must implement exactly one IActivity<TInput, TOutput>."
            ),
        };
    }
}
