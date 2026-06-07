namespace PgWorkflows.Workflows;

[AttributeUsage(AttributeTargets.Class)]
public sealed class WorkflowAttribute(string? name = null) : Attribute
{
    public string? Name { get; } = name;
}
