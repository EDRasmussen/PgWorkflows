namespace PgWorkflows.Workflows;

/// <summary>
/// Thrown by <see cref="IWorkflowContext.Sleep"/> to unwind the workflow so the runner can park
/// the run until the timer fires. This is control flow, not an error: the runner catches it and
/// re-schedules the run via <c>visible_at</c> rather than recording a failure. The timer's
/// deadline is persisted atomically with the park, so this exception carries the sequence and
/// fire time the runner needs.
/// </summary>
internal sealed class WorkflowSleepException(int timerSequence, DateTimeOffset fireAt) : Exception
{
    public int TimerSequence { get; } = timerSequence;

    public DateTimeOffset FireAt { get; } = fireAt;
}
