using System.Collections.Concurrent;

namespace PgWorkflows.Activities;

public sealed class ActivityRegistry
{
    private readonly ConcurrentDictionary<string, ActivityHandler> _handlers = new(
        StringComparer.Ordinal
    );

    public void Register(string activityName, ActivityHandler handler)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(activityName);
        ArgumentNullException.ThrowIfNull(handler);

        if (!_handlers.TryAdd(activityName, handler))
        {
            throw new InvalidOperationException(
                $"An activity named '{activityName}' is already registered."
            );
        }
    }

    public bool TryResolve(string activityName, out ActivityHandler? handler)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(activityName);
        return _handlers.TryGetValue(activityName, out handler);
    }
}
