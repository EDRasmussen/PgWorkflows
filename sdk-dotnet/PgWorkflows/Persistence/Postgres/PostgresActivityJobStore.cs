using Npgsql;
using NpgsqlTypes;
using PgWorkflows.Jobs;

namespace PgWorkflows.Persistence.Postgres;

public sealed class PostgresActivityJobStore : IActivityJobStore
{
    private readonly NpgsqlDataSource _dataSource;

    public PostgresActivityJobStore(NpgsqlDataSource dataSource)
    {
        _dataSource = dataSource ?? throw new ArgumentNullException(nameof(dataSource));
    }

    public async ValueTask EnsureSchemaAsync(CancellationToken cancellationToken = default)
    {
        await using var connection = await _dataSource.OpenConnectionAsync(cancellationToken);
        await using var command = new NpgsqlCommand(PostgresSchema.Sql, connection);
        await command.ExecuteNonQueryAsync(cancellationToken);
    }

    public async ValueTask<Guid> EnqueueAsync(
        string activityName,
        string? inputJson,
        int maxAttempts = 1,
        DateTimeOffset? visibleAt = null,
        string? idempotencyKey = null,
        CancellationToken cancellationToken = default
    )
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(activityName);
        if (idempotencyKey is not null && string.IsNullOrWhiteSpace(idempotencyKey))
        {
            throw new ArgumentException("Idempotency key cannot be empty or whitespace.", nameof(idempotencyKey));
        }

        var jobId = Guid.NewGuid();
        var createdAt = DateTimeOffset.UtcNow;
        var resolvedVisibleAt = visibleAt ?? createdAt;

        const string sql = """
            insert into pw_activity_jobs (
                job_id,
                activity_name,
                idempotency_key,
                input,
                status,
                attempt,
                max_attempts,
                created_at,
                visible_at,
                lease_token,
                lease_expires_at,
                completed_at,
                result,
                error)
            values (
                @job_id,
                @activity_name,
                @idempotency_key,
                @input,
                @status,
                @attempt,
                @max_attempts,
                @created_at,
                @visible_at,
                null,
                null,
                null,
                null,
                null)
            on conflict (activity_name, idempotency_key)
                where idempotency_key is not null
            do update set idempotency_key = excluded.idempotency_key
            returning job_id;
            """;

        await using var command = _dataSource.CreateCommand(sql);
        command.Parameters.AddWithValue("job_id", jobId);
        command.Parameters.AddWithValue("activity_name", activityName);
        command.Parameters.AddWithValue(
            "idempotency_key",
            NpgsqlDbType.Text,
            (object?)idempotencyKey ?? DBNull.Value
        );
        command.Parameters.AddWithValue(
            "input",
            NpgsqlDbType.Jsonb,
            (object?)inputJson ?? DBNull.Value
        );
        command.Parameters.AddWithValue("status", PendingStatus);
        command.Parameters.AddWithValue("attempt", 0);
        command.Parameters.AddWithValue("max_attempts", Math.Max(maxAttempts, 1));
        command.Parameters.AddWithValue("created_at", createdAt);
        command.Parameters.AddWithValue("visible_at", resolvedVisibleAt);

        await using var reader = await command.ExecuteReaderAsync(cancellationToken);
        if (!await reader.ReadAsync(cancellationToken))
        {
            throw new InvalidOperationException("Enqueue did not return a job id.");
        }

        return reader.GetGuid(0);
    }

    public async ValueTask<IReadOnlyList<LeasedActivityJob>> LeaseAsync(
        string workerId,
        int batchSize,
        TimeSpan leaseDuration,
        DateTimeOffset now,
        CancellationToken cancellationToken = default
    )
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(workerId);

        var leaseToken = $"{workerId}/{Guid.NewGuid():N}";
        var leaseExpiresAt = now + leaseDuration;

        const string sql = """
            with candidate_jobs as (
                select job_id
                from pw_activity_jobs
                where visible_at <= @now
                  and (
                    status = @pending_status
                    or (status = @leased_status and lease_expires_at <= @now)
                  )
                order by visible_at, created_at
                limit @batch_size
                for update skip locked
            )
            update pw_activity_jobs jobs
            set status = @leased_status,
                attempt = jobs.attempt + 1,
                lease_token = @lease_token,
                lease_expires_at = @lease_expires_at,
                error = null
            from candidate_jobs
            where jobs.job_id = candidate_jobs.job_id
            returning
                jobs.job_id,
                jobs.activity_name,
                jobs.idempotency_key,
                jobs.input,
                jobs.attempt,
                jobs.max_attempts,
                jobs.created_at,
                jobs.lease_token,
                jobs.lease_expires_at;
            """;

        var jobs = new List<LeasedActivityJob>();

        await using var connection = await _dataSource.OpenConnectionAsync(cancellationToken);
        await using var command = new NpgsqlCommand(sql, connection);
        command.Parameters.AddWithValue("now", now);
        command.Parameters.AddWithValue("pending_status", PendingStatus);
        command.Parameters.AddWithValue("leased_status", LeasedStatus);
        command.Parameters.AddWithValue("batch_size", Math.Max(batchSize, 1));
        command.Parameters.AddWithValue("lease_token", leaseToken);
        command.Parameters.AddWithValue("lease_expires_at", leaseExpiresAt);

        await using var reader = await command.ExecuteReaderAsync(cancellationToken);
        while (await reader.ReadAsync(cancellationToken))
        {
            jobs.Add(
                new LeasedActivityJob(
                    reader.GetGuid(0),
                    reader.GetString(1),
                    ReadNullableString(reader, 3),
                    reader.GetInt32(4),
                    reader.GetInt32(5),
                    reader.GetFieldValue<DateTimeOffset>(6),
                    reader.GetString(7),
                    reader.GetFieldValue<DateTimeOffset>(8),
                    ReadNullableString(reader, 2)
                )
            );
        }

        return jobs;
    }

    public async ValueTask<ActivityJob?> GetAsync(
        Guid jobId,
        CancellationToken cancellationToken = default
    )
    {
        const string sql = """
            select
                job_id,
                activity_name,
                idempotency_key,
                input,
                status,
                attempt,
                max_attempts,
                created_at,
                visible_at,
                result,
                error
            from pw_activity_jobs
            where job_id = @job_id;
            """;

        await using var connection = await _dataSource.OpenConnectionAsync(cancellationToken);
        await using var command = new NpgsqlCommand(sql, connection);
        command.Parameters.AddWithValue("job_id", jobId);

        await using var reader = await command.ExecuteReaderAsync(cancellationToken);
        if (!await reader.ReadAsync(cancellationToken))
        {
            return null;
        }

        return new ActivityJob(
            reader.GetGuid(0),
            reader.GetString(1),
            ReadNullableString(reader, 3),
            ReadStatus(reader.GetString(4)),
            reader.GetInt32(5),
            reader.GetInt32(6),
            reader.GetFieldValue<DateTimeOffset>(7),
            reader.GetFieldValue<DateTimeOffset>(8),
            ReadNullableString(reader, 9),
            ReadNullableString(reader, 10),
            ReadNullableString(reader, 2)
        );
    }

    public ValueTask<bool> RenewLeaseAsync(
        Guid jobId,
        string leaseToken,
        DateTimeOffset leaseExpiresAt,
        CancellationToken cancellationToken = default
    ) =>
        ExecuteLeaseGuardedUpdateAsync(
            "lease_expires_at = @lease_expires_at",
            parameters => parameters.AddWithValue("lease_expires_at", leaseExpiresAt),
            jobId,
            leaseToken,
            cancellationToken
        );

    public ValueTask<bool> RecordSuccessAsync(
        Guid jobId,
        string leaseToken,
        string? resultJson,
        CancellationToken cancellationToken = default
    ) =>
        ExecuteLeaseGuardedUpdateAsync(
            """
            status = @status,
                result = @result,
                error = null,
                lease_token = null,
                lease_expires_at = null,
                completed_at = @completed_at
            """,
            parameters =>
            {
                parameters.AddWithValue("status", SucceededStatus);
                parameters.AddWithValue(
                    "result",
                    NpgsqlDbType.Jsonb,
                    (object?)resultJson ?? DBNull.Value
                );
                parameters.AddWithValue("completed_at", DateTimeOffset.UtcNow);
            },
            jobId,
            leaseToken,
            cancellationToken
        );

    public ValueTask<bool> RecordFailureAsync(
        Guid jobId,
        string leaseToken,
        string error,
        bool retryable,
        DateTimeOffset? nextVisibleAt,
        CancellationToken cancellationToken = default
    )
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(error);

        var status = retryable ? PendingStatus : FailedStatus;

        return ExecuteLeaseGuardedUpdateAsync(
            """
            status = @status,
                visible_at = @visible_at,
                error = @error,
                lease_token = null,
                lease_expires_at = null,
                completed_at = @completed_at
            """,
            parameters =>
            {
                parameters.AddWithValue("status", status);
                parameters.AddWithValue(
                    "visible_at",
                    retryable ? nextVisibleAt ?? DateTimeOffset.UtcNow : DateTimeOffset.UtcNow
                );
                parameters.AddWithValue("error", error);
                parameters.AddWithValue(
                    "completed_at",
                    retryable ? DBNull.Value : DateTimeOffset.UtcNow
                );
            },
            jobId,
            leaseToken,
            cancellationToken
        );
    }

    /// <summary>
    /// Runs an UPDATE guarded by the lease invariant — only the current lease holder, while
    /// the job is still leased, may mutate it. Returns <c>false</c> when no row matched (the
    /// lease was lost). The single source of truth for that guard; <paramref name="setClause"/>
    /// is always a compile-time constant, never caller input.
    /// </summary>
    private async ValueTask<bool> ExecuteLeaseGuardedUpdateAsync(
        string setClause,
        Action<NpgsqlParameterCollection> bindSetParameters,
        Guid jobId,
        string leaseToken,
        CancellationToken cancellationToken
    )
    {
        var sql = $"""
            update pw_activity_jobs
            set {setClause}
            where job_id = @job_id
              and lease_token = @lease_token
              and status = @leased_status;
            """;

        await using var connection = await _dataSource.OpenConnectionAsync(cancellationToken);
        await using var command = new NpgsqlCommand(sql, connection);
        bindSetParameters(command.Parameters);
        command.Parameters.AddWithValue("job_id", jobId);
        command.Parameters.AddWithValue("lease_token", leaseToken);
        command.Parameters.AddWithValue("leased_status", LeasedStatus);

        return await command.ExecuteNonQueryAsync(cancellationToken) > 0;
    }

    private static string? ReadNullableString(NpgsqlDataReader reader, int ordinal) =>
        reader.IsDBNull(ordinal) ? null : reader.GetString(ordinal);

    private static JobStatus ReadStatus(string value) =>
        value switch
        {
            PendingStatus => JobStatus.Pending,
            LeasedStatus => JobStatus.Leased,
            SucceededStatus => JobStatus.Succeeded,
            FailedStatus => JobStatus.Failed,
            _ => throw new InvalidOperationException($"Unknown job status '{value}'."),
        };

    private const string PendingStatus = "pending";
    private const string LeasedStatus = "leased";
    private const string SucceededStatus = "succeeded";
    private const string FailedStatus = "failed";
}
