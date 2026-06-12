using System.Reflection;
using System.Runtime.ExceptionServices;

namespace PgWorkflows.Internal;

/// <summary>
/// Shared reflection plumbing for invoking user-authored workflow and activity methods.
/// </summary>
internal static class ReflectionInvoke
{
    /// <summary>
    /// Invokes the method, unwrapping <see cref="TargetInvocationException"/> so the user's own
    /// exception propagates with its original stack trace — recorded errors must point at the
    /// user's code, not the reflection call site.
    /// </summary>
    public static object? InvokeUnwrapped(MethodInfo method, object? instance, object?[] args)
    {
        try
        {
            return method.Invoke(instance, args);
        }
        catch (TargetInvocationException ex) when (ex.InnerException is not null)
        {
            ExceptionDispatchInfo.Capture(ex.InnerException).Throw();
            throw;
        }
    }

    /// <summary>
    /// Awaits whatever the method returned (void, Task, ValueTask, or their generic forms) and
    /// yields the result value, or null for result-less returns.
    /// </summary>
    public static async ValueTask<object?> AwaitResultAsync(object? returned, Type returnType)
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

    /// <summary>
    /// Validates the shape rules every invokable user method shares: not generic, no by-ref
    /// parameters, and at most one CancellationToken which must come last.
    /// <paramref name="kind"/> labels the error, e.g. "Workflow run method" or "Activity method".
    /// </summary>
    public static void ValidateInvokableMethod(MethodInfo method, string kind)
    {
        var displayName = $"{method.DeclaringType?.FullName}.{method.Name}";

        if (method.IsGenericMethodDefinition)
        {
            throw new InvalidOperationException($"{kind} '{displayName}' must not be generic.");
        }

        var parameters = method.GetParameters();
        if (parameters.Any(parameter => parameter.ParameterType.IsByRef))
        {
            throw new InvalidOperationException(
                $"{kind} '{displayName}' must not use ref, out, or in parameters."
            );
        }

        var cancellationTokens = parameters
            .Where(parameter => parameter.ParameterType == typeof(CancellationToken))
            .ToArray();
        if (cancellationTokens.Length > 1)
        {
            throw new InvalidOperationException(
                $"{kind} '{displayName}' must accept at most one CancellationToken."
            );
        }

        if (
            cancellationTokens.Length == 1
            && parameters[^1].ParameterType != typeof(CancellationToken)
        )
        {
            throw new InvalidOperationException(
                $"{kind} '{displayName}' must put CancellationToken last."
            );
        }
    }
}
