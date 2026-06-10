using Npgsql;

namespace PgWorkflows.Persistence;

/// <summary>
/// Classifies exceptions that mean the database was unreachable or overloaded (connection
/// exhaustion, network drops, serialization failures) rather than the work itself being broken.
/// Such errors say nothing about the workflow, so they must not consume retry attempts.
/// </summary>
internal static class TransientErrors
{
    public static bool IsTransient(Exception exception)
    {
        for (Exception? current = exception; current is not null; current = current.InnerException)
        {
            if (current is NpgsqlException { IsTransient: true })
            {
                return true;
            }

            if (current is AggregateException aggregate)
            {
                return aggregate.InnerExceptions.Any(IsTransient);
            }
        }

        return false;
    }
}
