namespace PgWorkflows.Jobs;

internal enum JobStatus
{
    Pending = 0,
    Leased = 1,
    Succeeded = 2,
    Failed = 3,
}
