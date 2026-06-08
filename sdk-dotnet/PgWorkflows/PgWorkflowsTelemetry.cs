using System.Diagnostics;

namespace PgWorkflows;

public static class PgWorkflowsTelemetry
{
    public const string ActivitySourceName = "PgWorkflows";

    public static readonly ActivitySource ActivitySource = new(ActivitySourceName);
}
