using System.Diagnostics;
using System.Diagnostics.Metrics;

namespace PgWorkflows;

public static class PgWorkflowsTelemetry
{
    public const string ActivitySourceName = "PgWorkflows";
    public const string MeterName = "PgWorkflows";

    public static readonly ActivitySource ActivitySource = new(ActivitySourceName);

    internal static readonly Meter Meter = new(MeterName);

    internal static readonly Counter<long> LeasedJobs = Meter.CreateCounter<long>(
        "pgworkflows.activity.jobs.leased",
        description: "Number of activity jobs leased by workers."
    );

    internal static readonly Counter<long> Executions = Meter.CreateCounter<long>(
        "pgworkflows.activity.executions",
        description: "Number of activity job executions by outcome."
    );

    internal static readonly Counter<long> WorkerFailures = Meter.CreateCounter<long>(
        "pgworkflows.worker.failures",
        description: "Number of non-fatal worker loop failures."
    );

    internal static readonly Histogram<double> ExecutionDuration = Meter.CreateHistogram<double>(
        "pgworkflows.activity.execution.duration",
        unit: "ms",
        description: "Activity handler execution duration."
    );

    internal static readonly Histogram<double> LeaseAge = Meter.CreateHistogram<double>(
        "pgworkflows.activity.lease.age",
        unit: "ms",
        description: "Age of an activity job when leased."
    );
}
