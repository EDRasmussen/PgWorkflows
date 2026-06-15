using Npgsql;
using NpgsqlTypes;
using PgWorkflows.Jobs;

namespace PgWorkflows.Persistence.Postgres;

internal sealed class PostgresActivityJobStore : IActivityJobStore
{
    private readonly NpgsqlDataSource _dataSource;

    public PostgresActivityJobStore(NpgsqlDataSource dataSource)
    {
        _dataSource = dataSource ?? throw new ArgumentNullException(nameof(dataSource));
    }

    /// <summary>
    /// Stable advisory lock key serializing schema creation. Every process in the fleet runs
    /// <see cref="EnsureSchemaAsync"/> at startup; concurrent "if not exists" DDL takes
    /// conflicting table locks in varying orders and deadlocks Postgres, crashing workers that
    /// start simultaneously. With the lock, one process applies the script and the rest wait,
    /// then re-run it as a no-op.
    /// </summary>
    private const long SchemaAdvisoryLockKey = 0x7067_776f_726b_666c; // "pgworkfl"

    public async ValueTask EnsureSchemaAsync(CancellationToken cancellationToken = default)
    {
        await using var connection = await _dataSource.OpenConnectionAsync(cancellationToken);
        await using var transaction = await connection.BeginTransactionAsync(cancellationToken);

        await using (
            var lockCommand = new NpgsqlCommand(
                "select pg_advisory_xact_lock(@key);",
                connection,
                transaction
            )
        )
        {
            lockCommand.Parameters.AddWithValue("key", SchemaAdvisoryLockKey);
            await lockCommand.ExecuteNonQueryAsync(cancellationToken);
        }

        await using (
            var bookkeepingCommand = new NpgsqlCommand(
                PostgresSchema.MigrationsTableSql,
                connection,
                transaction
            )
        )
        {
            await bookkeepingCommand.ExecuteNonQueryAsync(cancellationToken);
        }

        var applied = new HashSet<int>();
        await using (
            var appliedCommand = new NpgsqlCommand(
                "select version from pw_schema_migrations;",
                connection,
                transaction
            )
        )
        await using (var reader = await appliedCommand.ExecuteReaderAsync(cancellationToken))
        {
            while (await reader.ReadAsync(cancellationToken))
            {
                applied.Add(reader.GetInt32(0));
            }
        }

        foreach (var migration in PostgresSchema.Migrations)
        {
            if (applied.Contains(migration.Version))
            {
                continue;
            }

            await using (
                var migrationCommand = new NpgsqlCommand(migration.Sql, connection, transaction)
            )
            {
                await migrationCommand.ExecuteNonQueryAsync(cancellationToken);
            }

            await using var recordCommand = new NpgsqlCommand(
                """
                insert into pw_schema_migrations (version, name, applied_at)
                values (@version, @name, @applied_at);
                """,
                connection,
                transaction
            );
            recordCommand.Parameters.AddWithValue("version", migration.Version);
            recordCommand.Parameters.AddWithValue("name", migration.Name);
            recordCommand.Parameters.AddWithValue("applied_at", DateTimeOffset.UtcNow);
            await recordCommand.ExecuteNonQueryAsync(cancellationToken);
        }

        await transaction.CommitAsync(cancellationToken);
    }

    public async ValueTask<Guid> EnqueueAsync(
        string activityName,
        string? inputJson,
        int maxAttempts = 1,
        DateTimeOffset? visibleAt = null,
        string? idempotencyKey = null,
        Guid? workflowRunId = null,
        CancellationToken cancellationToken = default
    )
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(activityName);
        if (idempotencyKey is not null && string.IsNullOrWhiteSpace(idempotencyKey))
        {
            throw new ArgumentException(
                "Idempotency key cannot be empty or whitespace.",
                nameof(idempotencyKey)
            );
        }

        var jobId = Guid.NewGuid();
        var createdAt = DateTimeOffset.UtcNow;
        var resolvedVisibleAt = visibleAt ?? createdAt;

        const string sql = """
            insert into pw_activity_jobs (
                job_id,
                activity_name,
                idempotency_key,
                workflow_run_id,
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
                @workflow_run_id,
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
            "workflow_run_id",
            NpgsqlDbType.Uuid,
            (object?)workflowRunId ?? DBNull.Value
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
                    ReadNullableString(reader, 2),
                    reader.GetInt32(3),
                    reader.GetInt32(4),
                    reader.GetFieldValue<DateTimeOffset>(5),
                    reader.GetString(6),
                    reader.GetFieldValue<DateTimeOffset>(7)
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

    public async ValueTask<IReadOnlyList<Guid>> RenewLeasesAsync(
        IReadOnlyList<(Guid JobId, string LeaseToken)> leases,
        DateTimeOffset leaseExpiresAt,
        CancellationToken cancellationToken = default
    )
    {
        if (leases.Count == 0)
        {
            return [];
        }

        var ids = new Guid[leases.Count];
        var tokens = new string[leases.Count];
        for (var i = 0; i < leases.Count; i++)
        {
            ids[i] = leases[i].JobId;
            tokens[i] = leases[i].LeaseToken;
        }

        const string sql = """
            update pw_activity_jobs
            set lease_expires_at = @lease_expires_at
            where status = @leased_status
              and (job_id, lease_token) in (
                  select * from unnest(@ids::uuid[], @tokens::text[]))
            returning job_id;
            """;

        await using var connection = await _dataSource.OpenConnectionAsync(cancellationToken);
        await using var command = new NpgsqlCommand(sql, connection);
        command.Parameters.AddWithValue("lease_expires_at", leaseExpiresAt);
        command.Parameters.AddWithValue("leased_status", LeasedStatus);
        command.Parameters.AddWithValue("ids", ids);
        command.Parameters.AddWithValue("tokens", tokens);

        var held = new List<Guid>(leases.Count);
        await using var reader = await command.ExecuteReaderAsync(cancellationToken);
        while (await reader.ReadAsync(cancellationToken))
        {
            held.Add(reader.GetFieldValue<Guid>(0));
        }

        return held;
    }

    public ValueTask<bool> RecordSuccessAsync(
        Guid jobId,
        string leaseToken,
        string? resultJson,
        CancellationToken cancellationToken = default
    ) =>
        RecordTerminalAndWakeAsync(
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

        return RecordTerminalAndWakeAsync(
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
    /// Records a terminal job outcome under the lease guard and, in the same transaction, wakes the
    /// parent run by pulling its parked <c>visible_at</c> forward — atomic, so a crash can't leave a
    /// completed job whose parent was never woken. Every completion wakes (not just the last
    /// sibling): a fan-out may resume-and-re-park a few times, harmless since replay is idempotent.
    /// Not gated on "no siblings remain" — that check races under concurrent completions and loses
    /// the wake. Returns <c>false</c> when the lease was lost.
    /// </summary>
    private async ValueTask<bool> RecordTerminalAndWakeAsync(
        string setClause,
        Action<NpgsqlParameterCollection> bindSetParameters,
        Guid jobId,
        string leaseToken,
        CancellationToken cancellationToken
    )
    {
        var now = DateTimeOffset.UtcNow;

        await using var connection = await _dataSource.OpenConnectionAsync(cancellationToken);
        await using var transaction = await connection.BeginTransactionAsync(cancellationToken);

        // Lock the parent run row first (the park does too) so completion and park serialize
        // run-then-job; otherwise the wake below races the park's recheck and is lost, stranding the
        // run until its grace deadline. (workflow_run_id is immutable once set.)
        Guid? workflowRunId;
        await using (
            var command = new NpgsqlCommand(
                "select workflow_run_id from pw_activity_jobs where job_id = @job_id;",
                connection,
                transaction
            )
        )
        {
            command.Parameters.AddWithValue("job_id", jobId);
            workflowRunId = await command.ExecuteScalarAsync(cancellationToken) is Guid id
                ? id
                : null;
        }

        if (workflowRunId is { } lockRunId)
        {
            await using var command = new NpgsqlCommand(
                "select 1 from pw_workflow_runs where workflow_run_id = @workflow_run_id for update;",
                connection,
                transaction
            );
            command.Parameters.AddWithValue("workflow_run_id", lockRunId);
            await command.ExecuteScalarAsync(cancellationToken);
        }

        var terminalSql = $"""
            update pw_activity_jobs
            set {setClause}
            where job_id = @job_id
              and lease_token = @lease_token
              and status = @leased_status;
            """;

        await using (var command = new NpgsqlCommand(terminalSql, connection, transaction))
        {
            bindSetParameters(command.Parameters);
            command.Parameters.AddWithValue("job_id", jobId);
            command.Parameters.AddWithValue("lease_token", leaseToken);
            command.Parameters.AddWithValue("leased_status", LeasedStatus);

            if (await command.ExecuteNonQueryAsync(cancellationToken) == 0)
            {
                // Lease lost: another worker reclaimed the job. Write nothing and abandon.
                await transaction.RollbackAsync(cancellationToken);
                return false;
            }
        }

        // Wake the parent run by pulling its parked deadline forward. The visible_at > @now guard
        // only pulls earlier, never later, so it's idempotent across concurrent sibling completions
        // and a no-op for a run that isn't parked.
        if (workflowRunId is { } wakeRunId)
        {
            const string wakeSql = """
                update pw_workflow_runs
                set visible_at = @now,
                    updated_at = @now
                where workflow_run_id = @workflow_run_id
                  and status = @pending_status
                  and lease_token is null
                  and visible_at > @now;
                """;

            await using var command = new NpgsqlCommand(wakeSql, connection, transaction);
            command.Parameters.AddWithValue("workflow_run_id", wakeRunId);
            command.Parameters.AddWithValue("now", now);
            command.Parameters.AddWithValue("pending_status", PendingStatus);

            await command.ExecuteNonQueryAsync(cancellationToken);
        }

        await transaction.CommitAsync(cancellationToken);
        return true;
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
