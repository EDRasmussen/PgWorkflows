namespace PgWorkflows.Activities;

/// <summary>Low-level handler for already-serialized JSON activity payloads.</summary>
public delegate ValueTask<string?> ActivityHandler(
    ActivityExecutionContext context,
    string? inputJson,
    CancellationToken cancellationToken);
