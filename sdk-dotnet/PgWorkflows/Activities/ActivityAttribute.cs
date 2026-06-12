namespace PgWorkflows.Activities;

/// <summary>
/// Marks a public method as an activity discovered by
/// <c>AddActivities&lt;TActivities&gt;()</c>. The optional <paramref name="name"/> is the
/// activity's durable identity in the job queue; it defaults to the method name. Activity names
/// must be unique across the registration.
/// </summary>
[AttributeUsage(AttributeTargets.Method, AllowMultiple = false, Inherited = false)]
public sealed class ActivityAttribute(string? name = null) : Attribute
{
    /// <summary>The activity's durable name, or null to use the method name.</summary>
    public string? Name { get; } = name;
}
