namespace PgWorkflows.Activities;

[AttributeUsage(AttributeTargets.Method, AllowMultiple = false, Inherited = false)]
public sealed class ActivityAttribute(string? name = null) : Attribute
{
    public string? Name { get; } = name;
}
