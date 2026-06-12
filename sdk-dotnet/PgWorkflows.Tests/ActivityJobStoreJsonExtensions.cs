using System.Text.Json;
using PgWorkflows.Persistence;

namespace PgWorkflows.Tests;

/// <summary>Test convenience for enqueueing raw activity jobs with a typed, JSON-serialized input.</summary>
internal static class ActivityJobStoreJsonExtensions
{
    public static ValueTask<Guid> EnqueueTypedAsync<TInput>(
        this IActivityJobStore store,
        string activityName,
        TInput input,
        int maxAttempts = 1,
        DateTimeOffset? visibleAt = null,
        JsonSerializerOptions? jsonSerializerOptions = null,
        CancellationToken cancellationToken = default,
        string? idempotencyKey = null
    )
    {
        ArgumentNullException.ThrowIfNull(store);
        var serializedInput = JsonSerializer.Serialize(input, jsonSerializerOptions);

        return store.EnqueueAsync(
            activityName,
            serializedInput,
            maxAttempts,
            visibleAt,
            idempotencyKey,
            cancellationToken: cancellationToken
        );
    }
}
