namespace PgWorkflows.Workflows;

/// <summary>
/// Thrown by <see cref="IWorkflowContext.WaitForSignal{TSignal}"/> to unwind a leased workflow
/// run while it waits for an external signal. The runner catches it and parks the run, releasing
/// the workflow lease; signal delivery wakes the run by making it immediately visible again.
/// </summary>
internal sealed class WorkflowSignalWaitException(int waitSequence, string signalName) : Exception
{
    public int WaitSequence { get; } = waitSequence;

    public string SignalName { get; } = signalName;
}
