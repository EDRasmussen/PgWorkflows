using System.Linq.Expressions;
using System.Reflection;
using System.Text.Json;
using PgWorkflows.Activities;

namespace PgWorkflows.Workflows;

internal sealed record WorkflowActivityCall(string ActivityName, string? InputJson)
{
    public static WorkflowActivityCall FromExpression<TActivities, TOutput>(
        Expression<Func<TActivities, TOutput>> expression,
        JsonSerializerOptions? jsonSerializerOptions
    ) =>
        FromExpressionBody(expression.Body, jsonSerializerOptions);

    public static WorkflowActivityCall FromExpression<TActivities, TOutput>(
        Expression<Func<TActivities, Task<TOutput>>> expression,
        JsonSerializerOptions? jsonSerializerOptions
    ) =>
        FromExpressionBody(expression.Body, jsonSerializerOptions);

    public static WorkflowActivityCall FromExpression<TActivities, TOutput>(
        Expression<Func<TActivities, ValueTask<TOutput>>> expression,
        JsonSerializerOptions? jsonSerializerOptions
    ) =>
        FromExpressionBody(expression.Body, jsonSerializerOptions);

    private static WorkflowActivityCall FromExpressionBody(
        Expression body,
        JsonSerializerOptions? jsonSerializerOptions
    )
    {
        var call = body switch
        {
            MethodCallExpression methodCall => methodCall,
            UnaryExpression { Operand: MethodCallExpression methodCall } => methodCall,
            _ => throw new InvalidOperationException(
                "Workflow activity calls must be direct method calls, for example: ctx.Activity((MyActivities a) => a.DoWork(input))."
            ),
        };

        var activityName = GetActivityName(call.Method);
        var inputJson = GetInputJson(call, jsonSerializerOptions);
        return new WorkflowActivityCall(activityName, inputJson);
    }

    private static string GetActivityName(MethodInfo method)
    {
        var attribute = method.GetCustomAttribute<ActivityAttribute>();
        return attribute is not null && !string.IsNullOrWhiteSpace(attribute.Name)
            ? attribute.Name!
            : method.Name;
    }

    private static string? GetInputJson(
        MethodCallExpression call,
        JsonSerializerOptions? jsonSerializerOptions
    )
    {
        var parameters = call.Method.GetParameters();
        var inputArguments = call
            .Arguments.Select((argument, index) => new { Argument = argument, Parameter = parameters[index] })
            .Where(item => item.Parameter.ParameterType != typeof(CancellationToken))
            .ToArray();

        if (inputArguments.Length > 1)
        {
            throw new InvalidOperationException(
                $"Activity method '{call.Method.DeclaringType?.FullName}.{call.Method.Name}' must accept at most one input argument before CancellationToken."
            );
        }

        if (inputArguments.Length == 0)
        {
            return null;
        }

        var input = inputArguments[0];
        var value = Evaluate(input.Argument);
        return JsonSerializer.Serialize(value, input.Parameter.ParameterType, jsonSerializerOptions);
    }

    private static object? Evaluate(Expression expression)
    {
        var converted = Expression.Convert(expression, typeof(object));
        return Expression.Lambda<Func<object?>>(converted).Compile().Invoke();
    }
}
