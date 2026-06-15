using System.Reflection;
using Microsoft.Extensions.DependencyInjection;
using PgWorkflows.Internal;

namespace PgWorkflows.Workflows;

internal sealed class WorkflowRegistry
{
    private readonly object _gate = new();
    private readonly Dictionary<Type, WorkflowDefinition> _definitions = [];
    private readonly Dictionary<string, Type> _workflowTypesByName = new(StringComparer.Ordinal);

    public void Register<TWorkflow>()
        where TWorkflow : class
    {
        var workflowType = typeof(TWorkflow);
        var definition = CreateDefinition(workflowType);

        lock (_gate)
        {
            if (_definitions.ContainsKey(workflowType))
            {
                throw new InvalidOperationException(
                    $"Workflow type '{workflowType.FullName}' is already registered."
                );
            }

            if (_workflowTypesByName.TryGetValue(definition.Name, out var existingType))
            {
                throw new InvalidOperationException(
                    $"Workflow name '{definition.Name}' is already registered by '{existingType.FullName}'. Workflow names must be unique."
                );
            }

            _definitions.Add(workflowType, definition);
            _workflowTypesByName.Add(definition.Name, workflowType);
        }
    }

    internal WorkflowDefinition Resolve<TWorkflow>()
        where TWorkflow : class
    {
        var workflowType = typeof(TWorkflow);
        lock (_gate)
        {
            if (_definitions.TryGetValue(workflowType, out var definition))
            {
                return definition;
            }
        }

        throw new InvalidOperationException(
            $"Workflow type '{workflowType.FullName}' is not registered. Call AddWorkflow<{workflowType.Name}>() during AddPgWorkflows configuration."
        );
    }

    internal bool TryResolve(string workflowName, out WorkflowDefinition? definition)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(workflowName);

        lock (_gate)
        {
            if (
                _workflowTypesByName.TryGetValue(workflowName, out var workflowType)
                && _definitions.TryGetValue(workflowType, out definition)
            )
            {
                return true;
            }
        }

        definition = null;
        return false;
    }

    private static WorkflowDefinition CreateDefinition(Type workflowType)
    {
        var workflowAttribute =
            workflowType.GetCustomAttribute<WorkflowAttribute>()
            ?? throw new InvalidOperationException(
                $"Workflow type '{workflowType.FullName}' must be marked with [Workflow]."
            );

        var runMethods = workflowType
            .GetMethods(BindingFlags.Instance | BindingFlags.Public)
            .Where(method => method.GetCustomAttribute<WorkflowRunAttribute>() is not null)
            .ToArray();

        if (runMethods.Length != 1)
        {
            throw new InvalidOperationException(
                $"Workflow type '{workflowType.FullName}' must define exactly one public method marked with [WorkflowRun]."
            );
        }

        var runMethod = runMethods[0];
        ValidateRunMethod(runMethod);
        var inputType = GetInputType(runMethod);
        var outputType = GetOutputType(runMethod.ReturnType);
        var workflowName = string.IsNullOrWhiteSpace(workflowAttribute.Name)
            ? workflowType.Name
            : workflowAttribute.Name!;

        return new WorkflowDefinition(
            workflowType,
            workflowName,
            inputType,
            outputType,
            CreateInvoker(workflowType, runMethod)
        );
    }

    private static void ValidateRunMethod(MethodInfo method)
    {
        ReflectionInvoke.ValidateInvokableMethod(method, "Workflow run method");

        if (method.GetParameters().Count(p => p.ParameterType == typeof(IWorkflowContext)) != 1)
        {
            throw new InvalidOperationException(
                $"Workflow run method '{method.DeclaringType?.FullName}.{method.Name}' must accept exactly one IWorkflowContext parameter."
            );
        }

        _ = GetInputType(method);
        _ = GetOutputType(method.ReturnType);
    }

    private static Type GetInputType(MethodInfo method)
    {
        var inputParameters = method
            .GetParameters()
            .Where(parameter =>
                parameter.ParameterType != typeof(IWorkflowContext)
                && parameter.ParameterType != typeof(CancellationToken)
            )
            .ToArray();

        if (inputParameters.Length != 1)
        {
            throw new InvalidOperationException(
                $"Workflow run method '{method.DeclaringType?.FullName}.{method.Name}' must accept exactly one input parameter."
            );
        }

        return inputParameters[0].ParameterType;
    }

    private static Type GetOutputType(Type returnType)
    {
        if (
            returnType == typeof(void)
            || returnType == typeof(Task)
            || returnType == typeof(ValueTask)
        )
        {
            return typeof(object);
        }

        if (
            returnType.IsGenericType
            && returnType.GetGenericTypeDefinition() == typeof(ValueTask<>)
        )
        {
            return returnType.GetGenericArguments()[0];
        }

        if (returnType.IsGenericType && returnType.GetGenericTypeDefinition() == typeof(Task<>))
        {
            return returnType.GetGenericArguments()[0];
        }

        return returnType;
    }

    private static Func<
        IServiceProvider,
        IWorkflowContext,
        object?,
        CancellationToken,
        ValueTask<object?>
    > CreateInvoker(Type workflowType, MethodInfo method)
    {
        var parameters = method.GetParameters();

        return async (provider, context, input, cancellationToken) =>
        {
            var workflow = ActivatorUtilities.GetServiceOrCreateInstance(provider, workflowType);
            var args = new object?[parameters.Length];

            for (var i = 0; i < parameters.Length; i++)
            {
                var parameterType = parameters[i].ParameterType;
                if (parameterType == typeof(IWorkflowContext))
                {
                    args[i] = context;
                }
                else if (parameterType == typeof(CancellationToken))
                {
                    args[i] = cancellationToken;
                }
                else
                {
                    args[i] = input;
                }
            }

            var returned = ReflectionInvoke.InvokeUnwrapped(method, workflow, args);
            return await ReflectionInvoke.AwaitResultAsync(returned, method.ReturnType);
        };
    }
}
