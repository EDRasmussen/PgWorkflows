namespace PgWorkflows.Activities;

public delegate ValueTask<string?> ActivityHandler(
    ActivityExecutionContext context,
    string? input,
    CancellationToken cancellationToken);
