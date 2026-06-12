namespace PgWorkflows.Workflows;

/// <summary>
/// What a single leased pass over a workflow run achieved. A parked outcome is success from the
/// worker's perspective: the run durably yielded and will be resumed later.
/// </summary>
internal enum WorkflowExecutionOutcome
{
    /// <summary>The run finished and its result was recorded.</summary>
    Completed,

    /// <summary>The run parked on a durable timer (<c>ctx.Sleep</c>).</summary>
    Sleeping,

    /// <summary>The run parked waiting for an external signal (<c>ctx.WaitForSignal</c>).</summary>
    WaitingForSignal,

    /// <summary>The run parked waiting on outstanding activity steps.</summary>
    WaitingForActivities,

    /// <summary>
    /// The lease was lost while recording the outcome; nothing was written and another worker
    /// owns the run now.
    /// </summary>
    LeaseLost,
}
