namespace PgWorkflows.Workflows;

/// <summary>
/// Marks a class as a workflow definition. The optional <paramref name="name"/> is the workflow's
/// durable identity in the database; it defaults to the class name. Renaming the class without
/// pinning the name orphans in-flight runs, so set it explicitly for anything long-lived.
/// </summary>
[AttributeUsage(AttributeTargets.Class)]
public sealed class WorkflowAttribute(string? name = null) : Attribute
{
    /// <summary>The workflow's durable name, or null to use the class name.</summary>
    public string? Name { get; } = name;
}
