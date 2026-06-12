using System.Diagnostics;

namespace PgWorkflows;

/// <summary>
/// OpenTelemetry integration point. Subscribe to <see cref="ActivitySourceName"/> in your tracer
/// configuration to receive spans for activity and workflow execution.
/// </summary>
public static class PgWorkflowsTelemetry
{
    /// <summary>The name to pass to <c>AddSource(...)</c> when configuring tracing.</summary>
    public const string ActivitySourceName = "PgWorkflows";

    /// <summary>The source all PgWorkflows spans are emitted from.</summary>
    public static readonly ActivitySource ActivitySource = new(ActivitySourceName);
}
