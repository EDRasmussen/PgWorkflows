using System.Text.Json;
using System.Text.Json.Serialization.Metadata;
using PgWorkflows.Jobs;

namespace PgWorkflows.Persistence;

public static class ActivityJobStoreJsonExtensions
{
    public static ValueTask<Guid> EnqueueAsync<TInput>(
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
            new EnqueueActivityRequest(activityName, serializedInput, maxAttempts, visibleAt, idempotencyKey),
            cancellationToken
        );
    }

    public static ValueTask<Guid> EnqueueAsync<TInput>(
        this IActivityJobStore store,
        string activityName,
        TInput input,
        JsonTypeInfo<TInput> inputJsonTypeInfo,
        int maxAttempts = 1,
        DateTimeOffset? visibleAt = null,
        CancellationToken cancellationToken = default,
        string? idempotencyKey = null
    )
    {
        ArgumentNullException.ThrowIfNull(store);
        ArgumentNullException.ThrowIfNull(inputJsonTypeInfo);
        var serializedInput = JsonSerializer.Serialize(input, inputJsonTypeInfo);

        return store.EnqueueAsync(
            new EnqueueActivityRequest(activityName, serializedInput, maxAttempts, visibleAt, idempotencyKey),
            cancellationToken
        );
    }
}
