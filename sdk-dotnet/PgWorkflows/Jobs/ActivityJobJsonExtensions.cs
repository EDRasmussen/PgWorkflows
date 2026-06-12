using System.Text.Json;
using System.Text.Json.Serialization.Metadata;

namespace PgWorkflows.Jobs;

internal static class ActivityJobJsonExtensions
{
    public static TOutput? GetResult<TOutput>(
        this ActivityJob job,
        JsonSerializerOptions? jsonSerializerOptions = null
    )
    {
        ArgumentNullException.ThrowIfNull(job);
        return job.ResultJson is null
            ? default
            : JsonSerializer.Deserialize<TOutput>(job.ResultJson, jsonSerializerOptions);
    }

    public static TOutput? GetResult<TOutput>(
        this ActivityJob job,
        JsonTypeInfo<TOutput> outputJsonTypeInfo
    )
    {
        ArgumentNullException.ThrowIfNull(job);
        ArgumentNullException.ThrowIfNull(outputJsonTypeInfo);
        return job.ResultJson is null
            ? default
            : JsonSerializer.Deserialize(job.ResultJson, outputJsonTypeInfo);
    }
}
