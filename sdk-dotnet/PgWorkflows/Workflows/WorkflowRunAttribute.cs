namespace PgWorkflows.Workflows;

/// <summary>
/// Marks the single public entry-point method of a <see cref="WorkflowAttribute"/> class. The
/// method takes an <see cref="IWorkflowContext"/>, exactly one input parameter, and optionally a
/// trailing <see cref="CancellationToken"/>; it may return void, Task, ValueTask, or their
/// generic forms.
/// </summary>
[AttributeUsage(AttributeTargets.Method)]
public sealed class WorkflowRunAttribute : Attribute;
