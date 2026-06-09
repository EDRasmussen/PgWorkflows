namespace PgWorkflows.Workflows;

/// <summary>
/// Thrown by the workflow context to unwind a leased run that is waiting on one or more activity
/// steps to finish. Like <see cref="WorkflowSleepException"/>, this is control flow, not an error:
/// the runner catches it and parks the run (releasing the lease) instead of recording a failure.
/// Unlike a sleep there is no timer — the run is woken by the edge-trigger when its last outstanding
/// activity job completes, with a safety-net grace deadline as the backstop. It therefore carries no
/// payload.
/// </summary>
internal sealed class WorkflowParkException : Exception;
