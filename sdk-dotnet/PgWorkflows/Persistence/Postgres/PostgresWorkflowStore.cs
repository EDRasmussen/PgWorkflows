using Npgsql;
using NpgsqlTypes;
using PgWorkflows.Workflows;

namespace PgWorkflows.Persistence.Postgres;

internal sealed class PostgresWorkflowStore(NpgsqlDataSource dataSource) : IWorkflowStore
{
    private readonly NpgsqlDataSource _dataSource =
        dataSource ?? throw new ArgumentNullException(nameof(dataSource));

    public async ValueTask<Guid> CreateRunAsync(
        CreateWorkflowRunRequest request,
        CancellationToken cancellationToken = default
    )
    {
        ArgumentNullException.ThrowIfNull(request);
        ArgumentException.ThrowIfNullOrWhiteSpace(request.WorkflowName);
        if (request.IdempotencyKey is not null && string.IsNullOrWhiteSpace(request.IdempotencyKey))
        {
            throw new ArgumentException(
                "Idempotency key cannot be empty or whitespace.",
                nameof(request)
            );
        }

        var workflowRunId = Guid.NewGuid();
        var now = DateTimeOffset.UtcNow;

        const string sql = """
            insert into pw_workflow_runs (
                workflow_run_id,
                workflow_name,
                idempotency_key,
                input,
                status,
                attempt,
                max_attempts,
                visible_at,
                created_at,
                updated_at,
                completed_at,
                result,
                error)
            values (
                @workflow_run_id,
                @workflow_name,
                @idempotency_key,
                @input,
                @status,
                @attempt,
                @max_attempts,
                @visible_at,
                @created_at,
                @updated_at,
                null,
                null,
                null)
            on conflict (workflow_name, idempotency_key)
                where idempotency_key is not null
            do update set idempotency_key = excluded.idempotency_key
            returning workflow_run_id;
            """;

        await using var command = _dataSource.CreateCommand(sql);
        command.Parameters.AddWithValue("workflow_run_id", workflowRunId);
        command.Parameters.AddWithValue("workflow_name", request.WorkflowName);
        command.Parameters.AddWithValue(
            "idempotency_key",
            NpgsqlDbType.Text,
            (object?)request.IdempotencyKey ?? DBNull.Value
        );
        command.Parameters.AddWithValue(
            "input",
            NpgsqlDbType.Jsonb,
            (object?)request.InputJson ?? DBNull.Value
        );
        command.Parameters.AddWithValue("status", PendingStatus);
        command.Parameters.AddWithValue("attempt", 0);
        command.Parameters.AddWithValue("max_attempts", 1);
        command.Parameters.AddWithValue("visible_at", now);
        command.Parameters.AddWithValue("created_at", now);
        command.Parameters.AddWithValue("updated_at", now);

        var returned = await command.ExecuteScalarAsync(cancellationToken);
        return (Guid)returned!;
    }

    public async ValueTask<WorkflowRun?> GetRunAsync(
        Guid workflowRunId,
        CancellationToken cancellationToken = default
    )
    {
        const string sql = """
            select
                workflow_run_id,
                workflow_name,
                idempotency_key,
                input,
                status,
                attempt,
                max_attempts,
                visible_at,
                created_at,
                updated_at,
                completed_at,
                result,
                error
            from pw_workflow_runs
            where workflow_run_id = @workflow_run_id;
            """;

        await using var command = _dataSource.CreateCommand(sql);
        command.Parameters.AddWithValue("workflow_run_id", workflowRunId);

        await using var reader = await command.ExecuteReaderAsync(cancellationToken);
        if (!await reader.ReadAsync(cancellationToken))
        {
            return null;
        }

        return new WorkflowRun(
            reader.GetGuid(0),
            reader.GetString(1),
            ReadNullableString(reader, 3),
            ReadRunStatus(reader.GetString(4)),
            reader.GetFieldValue<DateTimeOffset>(8),
            reader.GetFieldValue<DateTimeOffset>(9),
            ReadNullableDateTimeOffset(reader, 10),
            ReadNullableString(reader, 11),
            ReadNullableString(reader, 12),
            ReadNullableString(reader, 2),
            reader.GetInt32(5),
            reader.GetInt32(6),
            reader.GetFieldValue<DateTimeOffset>(7)
        );
    }

    public async ValueTask<IReadOnlyList<LeasedWorkflowRun>> LeaseRunsAsync(
        string workerId,
        int limit,
        TimeSpan leaseDuration,
        DateTimeOffset now,
        int maxAttempts = 1,
        CancellationToken cancellationToken = default
    )
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(workerId);

        var clampedLimit = Math.Max(limit, 1);
        var leaseToken = Guid.NewGuid().ToString("N");
        var leaseExpiresAt = now.Add(leaseDuration);
        var staleUnleasedBefore = now.Subtract(leaseDuration);

        const string sql = """
            with leased as (
                select workflow_run_id
                from pw_workflow_runs
                where (status = @pending_status and visible_at <= @now)
                   or (status = @running_status and lease_expires_at <= @now)
                   or (status = @running_status and lease_expires_at is null and updated_at <= @stale_unleased_before)
                order by visible_at, created_at
                for update skip locked
                limit @limit
            )
            update pw_workflow_runs as runs
            set status = @running_status,
                attempt = runs.attempt + 1,
                max_attempts = greatest(runs.max_attempts, @max_attempts),
                updated_at = @now,
                workflow_worker_id = @worker_id,
                lease_token = @lease_token,
                lease_expires_at = @lease_expires_at,
                error = null
            from leased
            where runs.workflow_run_id = leased.workflow_run_id
            returning
                runs.workflow_run_id,
                runs.workflow_name,
                runs.attempt,
                runs.max_attempts,
                runs.lease_token,
                runs.lease_expires_at;
            """;

        await using var command = _dataSource.CreateCommand(sql);
        command.Parameters.AddWithValue("pending_status", PendingStatus);
        command.Parameters.AddWithValue("running_status", RunningStatus);
        command.Parameters.AddWithValue("now", now);
        command.Parameters.AddWithValue("stale_unleased_before", staleUnleasedBefore);
        command.Parameters.AddWithValue("limit", clampedLimit);
        command.Parameters.AddWithValue("max_attempts", Math.Max(maxAttempts, 1));
        command.Parameters.AddWithValue("worker_id", workerId);
        command.Parameters.AddWithValue("lease_token", leaseToken);
        command.Parameters.AddWithValue("lease_expires_at", leaseExpiresAt);

        var runs = new List<LeasedWorkflowRun>();
        await using var reader = await command.ExecuteReaderAsync(cancellationToken);
        while (await reader.ReadAsync(cancellationToken))
        {
            runs.Add(ReadLeasedWorkflowRun(reader));
        }

        return runs;
    }

    public async ValueTask<IReadOnlyList<Guid>> RenewRunLeasesAsync(
        IReadOnlyList<(Guid WorkflowRunId, string LeaseToken)> leases,
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
            ids[i] = leases[i].WorkflowRunId;
            tokens[i] = leases[i].LeaseToken;
        }

        const string sql = """
            update pw_workflow_runs
            set updated_at = @updated_at,
                lease_expires_at = @lease_expires_at
            where status = @running_status
              and (workflow_run_id, lease_token) in (
                  select * from unnest(@ids::uuid[], @tokens::text[]))
            returning workflow_run_id;
            """;

        await using var connection = await _dataSource.OpenConnectionAsync(cancellationToken);
        await using var command = new NpgsqlCommand(sql, connection);
        command.Parameters.AddWithValue("updated_at", DateTimeOffset.UtcNow);
        command.Parameters.AddWithValue("lease_expires_at", leaseExpiresAt);
        command.Parameters.AddWithValue("running_status", RunningStatus);
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

    public async ValueTask<bool> ReleaseRunAsync(
        Guid workflowRunId,
        string leaseToken,
        DateTimeOffset visibleAt,
        CancellationToken cancellationToken = default
    )
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(leaseToken);

        await using var command = _dataSource.CreateCommand(ReleaseRunSql);
        command.Parameters.AddWithValue("workflow_run_id", workflowRunId);
        command.Parameters.AddWithValue("lease_token", leaseToken);
        command.Parameters.AddWithValue("pending_status", PendingStatus);
        command.Parameters.AddWithValue("running_status", RunningStatus);
        command.Parameters.AddWithValue("visible_at", visibleAt);
        command.Parameters.AddWithValue("updated_at", DateTimeOffset.UtcNow);

        return await command.ExecuteNonQueryAsync(cancellationToken) == 1;
    }

    public ValueTask<bool> RecordRunSuccessAsync(
        Guid workflowRunId,
        string? resultJson,
        string leaseToken,
        CancellationToken cancellationToken = default
    ) =>
        UpdateLeasedRunAsync(
            workflowRunId,
            leaseToken,
            SucceededStatus,
            resultJson,
            error: null,
            completedAt: DateTimeOffset.UtcNow,
            cancellationToken
        );

    public ValueTask<bool> RecordRunFailureAsync(
        Guid workflowRunId,
        string error,
        string leaseToken,
        CancellationToken cancellationToken = default
    ) =>
        RecordRunFailureAsync(
            workflowRunId,
            error,
            leaseToken,
            retryable: false,
            nextVisibleAt: null,
            cancellationToken
        );

    public async ValueTask<bool> RecordRunFailureAsync(
        Guid workflowRunId,
        string error,
        string leaseToken,
        bool retryable,
        DateTimeOffset? nextVisibleAt,
        CancellationToken cancellationToken = default
    )
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(error);
        ArgumentException.ThrowIfNullOrWhiteSpace(leaseToken);

        var now = DateTimeOffset.UtcNow;
        var status = retryable ? PendingStatus : FailedStatus;

        const string sql = $"""
            update pw_workflow_runs
            set status = @status,
                updated_at = @updated_at,
                completed_at = @completed_at,
                result = null,
                error = @error,
                visible_at = @visible_at,
                {LeaseReleaseColumns}
            where workflow_run_id = @workflow_run_id
              and lease_token = @lease_token
              and status = @running_status;
            """;

        await using var command = _dataSource.CreateCommand(sql);
        command.Parameters.AddWithValue("workflow_run_id", workflowRunId);
        command.Parameters.AddWithValue("lease_token", leaseToken);
        command.Parameters.AddWithValue("running_status", RunningStatus);
        command.Parameters.AddWithValue("status", status);
        command.Parameters.AddWithValue("updated_at", now);
        command.Parameters.AddWithValue("completed_at", retryable ? DBNull.Value : now);
        command.Parameters.AddWithValue("error", error);
        command.Parameters.AddWithValue("visible_at", retryable ? nextVisibleAt ?? now : now);

        return await command.ExecuteNonQueryAsync(cancellationToken) == 1;
    }

    public async ValueTask<bool> RecordRunSleepingAsync(
        Guid workflowRunId,
        int timerSequence,
        DateTimeOffset fireAt,
        string leaseToken,
        CancellationToken cancellationToken = default
    )
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(leaseToken);

        var now = DateTimeOffset.UtcNow;

        await using var connection = await _dataSource.OpenConnectionAsync(cancellationToken);
        await using var transaction = await connection.BeginTransactionAsync(cancellationToken);

        await using (var parkCommand = new NpgsqlCommand(SleepParkSql, connection, transaction))
        {
            parkCommand.Parameters.AddWithValue("workflow_run_id", workflowRunId);
            parkCommand.Parameters.AddWithValue("lease_token", leaseToken);
            parkCommand.Parameters.AddWithValue("pending_status", PendingStatus);
            parkCommand.Parameters.AddWithValue("running_status", RunningStatus);
            parkCommand.Parameters.AddWithValue("fire_at", fireAt);
            parkCommand.Parameters.AddWithValue("updated_at", now);

            if (await parkCommand.ExecuteNonQueryAsync(cancellationToken) != 1)
            {
                // Lease lost: another worker now owns the run. Write nothing and abandon.
                await transaction.RollbackAsync(cancellationToken);
                return false;
            }
        }

        // Persist the deadline in the same transaction so the park and its durable fire time are
        // atomic.
        const string timerSql = """
            insert into pw_workflow_timers (
                workflow_run_id,
                timer_seq,
                fire_at,
                created_at)
            values (
                @workflow_run_id,
                @timer_seq,
                @fire_at,
                @created_at)
            on conflict (workflow_run_id, timer_seq) do nothing;
            """;

        await using (var timerCommand = new NpgsqlCommand(timerSql, connection, transaction))
        {
            timerCommand.Parameters.AddWithValue("workflow_run_id", workflowRunId);
            timerCommand.Parameters.AddWithValue("timer_seq", timerSequence);
            timerCommand.Parameters.AddWithValue("fire_at", fireAt);
            timerCommand.Parameters.AddWithValue("created_at", now);

            await timerCommand.ExecuteNonQueryAsync(cancellationToken);
        }

        await transaction.CommitAsync(cancellationToken);
        return true;
    }

    public async ValueTask<bool> RecordRunWaitingAsync(
        Guid workflowRunId,
        string leaseToken,
        TimeSpan grace,
        CancellationToken cancellationToken = default
    )
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(leaseToken);

        var now = DateTimeOffset.UtcNow;
        var graceDeadline = now + (grace > TimeSpan.Zero ? grace : TimeSpan.Zero);

        await using var connection = await _dataSource.OpenConnectionAsync(cancellationToken);
        await using var transaction = await connection.BeginTransactionAsync(cancellationToken);

        // The park UPDATE runs first so it acquires the run row lock before the recheck below,
        // mirroring the signal park's lock ordering.
        await using (
            var parkCommand = new NpgsqlCommand(ActivityWaitParkSql, connection, transaction)
        )
        {
            parkCommand.Parameters.AddWithValue("workflow_run_id", workflowRunId);
            parkCommand.Parameters.AddWithValue("lease_token", leaseToken);
            parkCommand.Parameters.AddWithValue("pending_status", PendingStatus);
            parkCommand.Parameters.AddWithValue("running_status", RunningStatus);
            parkCommand.Parameters.AddWithValue("updated_at", now);
            parkCommand.Parameters.AddWithValue("grace_deadline", graceDeadline);

            if (await parkCommand.ExecuteNonQueryAsync(cancellationToken) != 1)
            {
                // Lease lost: another worker now owns the run. Write nothing and abandon.
                await transaction.RollbackAsync(cancellationToken);
                return false;
            }
        }

        // Close the schedule→park lost-wake race: a completion committed during the park saw the run
        // still 'running' and skipped its wake. Because the park UPDATE holds the row lock, any such
        // completion either committed before this statement's snapshot (so the recheck sees the job
        // terminal and wakes the run) or is blocked on the lock and will see the parked run when it
        // resumes. One recheck covers the window; without it the run waits out its full grace deadline.
        const string recheckSql = """
            update pw_workflow_runs
            set visible_at = @now,
                updated_at = @now
            where workflow_run_id = @workflow_run_id
              and not exists (
                  select 1
                  from pw_activity_jobs job
                  where job.workflow_run_id = @workflow_run_id
                    and job.status in ('pending', 'leased'));
            """;

        await using (var recheckCommand = new NpgsqlCommand(recheckSql, connection, transaction))
        {
            recheckCommand.Parameters.AddWithValue("workflow_run_id", workflowRunId);
            recheckCommand.Parameters.AddWithValue("now", now);

            await recheckCommand.ExecuteNonQueryAsync(cancellationToken);
        }

        await transaction.CommitAsync(cancellationToken);
        return true;
    }

    public async ValueTask<bool> RecordRunWaitingForSignalAsync(
        Guid workflowRunId,
        int waitSequence,
        string signalName,
        string leaseToken,
        CancellationToken cancellationToken = default
    )
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(signalName);
        ArgumentException.ThrowIfNullOrWhiteSpace(leaseToken);

        var now = DateTimeOffset.UtcNow;

        await using var connection = await _dataSource.OpenConnectionAsync(cancellationToken);
        await using var transaction = await connection.BeginTransactionAsync(cancellationToken);

        // The park is open-ended (visible_at = infinity): signal delivery wakes the run via the
        // edge-trigger, so there's no point polling a wait that may take days. The park UPDATE runs
        // first so every signal transaction takes the run row before any other lock (same order as
        // ConsumeSignalAsync/RecordSignalAsync), ruling out deadlocks.
        await using (var parkCommand = new NpgsqlCommand(SignalParkSql, connection, transaction))
        {
            parkCommand.Parameters.AddWithValue("workflow_run_id", workflowRunId);
            parkCommand.Parameters.AddWithValue("lease_token", leaseToken);
            parkCommand.Parameters.AddWithValue("pending_status", PendingStatus);
            parkCommand.Parameters.AddWithValue("running_status", RunningStatus);
            parkCommand.Parameters.AddWithValue("visible_at", DateTimeOffset.MaxValue);
            parkCommand.Parameters.AddWithValue("updated_at", now);

            if (await parkCommand.ExecuteNonQueryAsync(cancellationToken) != 1)
            {
                // Lease lost: another worker now owns the run. Write nothing and abandon.
                await transaction.RollbackAsync(cancellationToken);
                return false;
            }
        }

        const string waitSql = """
            insert into pw_workflow_signal_waits (
                workflow_run_id,
                wait_seq,
                signal_name,
                created_at,
                completed_at)
            values (
                @workflow_run_id,
                @wait_seq,
                @signal_name,
                @created_at,
                null)
            on conflict (workflow_run_id, wait_seq) do update
            set signal_name = excluded.signal_name;
            """;

        await using (var waitCommand = new NpgsqlCommand(waitSql, connection, transaction))
        {
            waitCommand.Parameters.AddWithValue("workflow_run_id", workflowRunId);
            waitCommand.Parameters.AddWithValue("wait_seq", waitSequence);
            waitCommand.Parameters.AddWithValue("signal_name", signalName);
            waitCommand.Parameters.AddWithValue("created_at", now);

            await waitCommand.ExecuteNonQueryAsync(cancellationToken);
        }

        // Close the consume→park lost-wake race: a signal committed in that window saw the run still
        // 'running' and skipped its wake. Because RecordSignalAsync locks the run row before
        // inserting, any such signal either committed before the park UPDATE took the lock (visible
        // to this snapshot) or is blocked on it and will see the parked run when it resumes. One
        // re-check covers the window.
        const string recheckSql = """
            update pw_workflow_runs
            set visible_at = @now,
                updated_at = @now
            where workflow_run_id = @workflow_run_id
              and exists (
                  select 1
                  from pw_workflow_signals signal
                  where signal.workflow_run_id = @workflow_run_id
                    and signal.signal_name = @signal_name
                    and signal.consumed_by_wait_seq is null);
            """;

        await using (var recheckCommand = new NpgsqlCommand(recheckSql, connection, transaction))
        {
            recheckCommand.Parameters.AddWithValue("workflow_run_id", workflowRunId);
            recheckCommand.Parameters.AddWithValue("signal_name", signalName);
            recheckCommand.Parameters.AddWithValue("now", now);

            await recheckCommand.ExecuteNonQueryAsync(cancellationToken);
        }

        await transaction.CommitAsync(cancellationToken);
        return true;
    }

    public async ValueTask<DateTimeOffset?> GetTimerAsync(
        Guid workflowRunId,
        int timerSequence,
        CancellationToken cancellationToken = default
    )
    {
        const string sql = """
            select fire_at
            from pw_workflow_timers
            where workflow_run_id = @workflow_run_id
              and timer_seq = @timer_seq;
            """;

        await using var command = _dataSource.CreateCommand(sql);
        command.Parameters.AddWithValue("workflow_run_id", workflowRunId);
        command.Parameters.AddWithValue("timer_seq", timerSequence);

        await using var reader = await command.ExecuteReaderAsync(cancellationToken);
        if (!await reader.ReadAsync(cancellationToken))
        {
            return null;
        }

        return reader.GetFieldValue<DateTimeOffset>(0);
    }

    public async ValueTask<string?> ConsumeSignalAsync(
        Guid workflowRunId,
        int waitSequence,
        string signalName,
        string leaseToken,
        CancellationToken cancellationToken = default
    )
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(signalName);
        ArgumentException.ThrowIfNullOrWhiteSpace(leaseToken);

        var now = DateTimeOffset.UtcNow;
        await using var connection = await _dataSource.OpenConnectionAsync(cancellationToken);
        await using var transaction = await connection.BeginTransactionAsync(cancellationToken);

        // Lock the run row and verify the lease in one statement, serializing consumption against
        // RecordSignalAsync and fencing out a worker whose lease was taken over — otherwise two
        // executors at the same wait could each claim a different signal, diverging from replay.
        const string leaseSql = """
            select 1
            from pw_workflow_runs
            where workflow_run_id = @workflow_run_id
              and lease_token = @lease_token
            for update;
            """;

        await using (var leaseCommand = new NpgsqlCommand(leaseSql, connection, transaction))
        {
            leaseCommand.Parameters.AddWithValue("workflow_run_id", workflowRunId);
            leaseCommand.Parameters.AddWithValue("lease_token", leaseToken);

            if (await leaseCommand.ExecuteScalarAsync(cancellationToken) is null)
            {
                // Lease lost: abandon without consuming. The caller unwinds to park, which is also
                // lease-guarded and writes nothing.
                await transaction.RollbackAsync(cancellationToken);
                return null;
            }
        }

        const string replaySql = """
            select payload
            from pw_workflow_signals
            where workflow_run_id = @workflow_run_id
              and signal_name = @signal_name
              and consumed_by_wait_seq = @wait_seq
            order by signal_seq
            limit 1;
            """;

        await using (var replayCommand = new NpgsqlCommand(replaySql, connection, transaction))
        {
            replayCommand.Parameters.AddWithValue("workflow_run_id", workflowRunId);
            replayCommand.Parameters.AddWithValue("signal_name", signalName);
            replayCommand.Parameters.AddWithValue("wait_seq", waitSequence);

            var replayed = await replayCommand.ExecuteScalarAsync(cancellationToken);
            if (replayed is not null and not DBNull)
            {
                await CompleteSignalWaitAsync(
                    connection,
                    transaction,
                    workflowRunId,
                    waitSequence,
                    now,
                    cancellationToken
                );
                await transaction.CommitAsync(cancellationToken);
                return (string)replayed;
            }
        }

        const string claimSql = """
            update pw_workflow_signals as signal
            set consumed_by_wait_seq = @wait_seq,
                consumed_at = @consumed_at
            where signal.workflow_run_id = @workflow_run_id
              and signal.signal_seq = (
                  select candidate.signal_seq
                  from pw_workflow_signals as candidate
                  where candidate.workflow_run_id = @workflow_run_id
                    and candidate.signal_name = @signal_name
                    and candidate.consumed_by_wait_seq is null
                  order by candidate.signal_seq
                  limit 1)
            returning signal.payload;
            """;

        await using var claimCommand = new NpgsqlCommand(claimSql, connection, transaction);
        claimCommand.Parameters.AddWithValue("workflow_run_id", workflowRunId);
        claimCommand.Parameters.AddWithValue("signal_name", signalName);
        claimCommand.Parameters.AddWithValue("wait_seq", waitSequence);
        claimCommand.Parameters.AddWithValue("consumed_at", now);

        var claimed = await claimCommand.ExecuteScalarAsync(cancellationToken);
        if (claimed is null or DBNull)
        {
            await transaction.CommitAsync(cancellationToken);
            return null;
        }

        await CompleteSignalWaitAsync(
            connection,
            transaction,
            workflowRunId,
            waitSequence,
            now,
            cancellationToken
        );
        await transaction.CommitAsync(cancellationToken);
        return (string)claimed;
    }

    public async ValueTask<Guid> RecordSignalAsync(
        Guid workflowRunId,
        string signalName,
        string payloadJson,
        string? idempotencyKey = null,
        CancellationToken cancellationToken = default
    )
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(signalName);
        ArgumentNullException.ThrowIfNull(payloadJson);
        if (idempotencyKey is not null && string.IsNullOrWhiteSpace(idempotencyKey))
        {
            throw new ArgumentException(
                "Idempotency key cannot be empty or whitespace.",
                nameof(idempotencyKey)
            );
        }

        var signalId = Guid.NewGuid();
        var now = DateTimeOffset.UtcNow;

        await using var connection = await _dataSource.OpenConnectionAsync(cancellationToken);
        await using var transaction = await connection.BeginTransactionAsync(cancellationToken);

        const string runSql = """
            select status
            from pw_workflow_runs
            where workflow_run_id = @workflow_run_id
            for update;
            """;

        string runStatus;
        await using (var runCommand = new NpgsqlCommand(runSql, connection, transaction))
        {
            runCommand.Parameters.AddWithValue("workflow_run_id", workflowRunId);
            var status = await runCommand.ExecuteScalarAsync(cancellationToken);
            if (status is null or DBNull)
            {
                await transaction.RollbackAsync(cancellationToken);
                throw new InvalidOperationException(
                    $"Workflow run '{workflowRunId}' was not found."
                );
            }

            runStatus = (string)status;
        }

        if (runStatus is SucceededStatus or FailedStatus)
        {
            await transaction.RollbackAsync(cancellationToken);
            throw new InvalidOperationException(
                $"Cannot signal workflow run '{workflowRunId}' because it is {runStatus}."
            );
        }

        const string insertSql = """
            insert into pw_workflow_signals (
                workflow_run_id,
                signal_id,
                signal_name,
                idempotency_key,
                payload,
                created_at,
                consumed_by_wait_seq,
                consumed_at)
            values (
                @workflow_run_id,
                @signal_id,
                @signal_name,
                @idempotency_key,
                @payload,
                @created_at,
                null,
                null)
            on conflict (workflow_run_id, signal_name, idempotency_key)
                where idempotency_key is not null
            do nothing
            returning signal_id;
            """;

        object? inserted;
        await using (var insertCommand = new NpgsqlCommand(insertSql, connection, transaction))
        {
            insertCommand.Parameters.AddWithValue("workflow_run_id", workflowRunId);
            insertCommand.Parameters.AddWithValue("signal_id", signalId);
            insertCommand.Parameters.AddWithValue("signal_name", signalName);
            insertCommand.Parameters.AddWithValue(
                "idempotency_key",
                NpgsqlDbType.Text,
                (object?)idempotencyKey ?? DBNull.Value
            );
            insertCommand.Parameters.AddWithValue("payload", NpgsqlDbType.Jsonb, payloadJson);
            insertCommand.Parameters.AddWithValue("created_at", now);

            inserted = await insertCommand.ExecuteScalarAsync(cancellationToken);
        }

        if (inserted is null or DBNull)
        {
            // Duplicate idempotent delivery: nothing new buffered, so skip the wake — it would only
            // force a replay that finds nothing to consume. Return the existing signal's id.
            const string existingSql = """
                select signal_id
                from pw_workflow_signals
                where workflow_run_id = @workflow_run_id
                  and signal_name = @signal_name
                  and idempotency_key = @idempotency_key;
                """;

            await using var existingCommand = new NpgsqlCommand(
                existingSql,
                connection,
                transaction
            );
            existingCommand.Parameters.AddWithValue("workflow_run_id", workflowRunId);
            existingCommand.Parameters.AddWithValue("signal_name", signalName);
            existingCommand.Parameters.AddWithValue("idempotency_key", idempotencyKey!);

            var existing = (Guid)(await existingCommand.ExecuteScalarAsync(cancellationToken))!;
            await transaction.CommitAsync(cancellationToken);
            return existing;
        }

        const string wakeSql = """
            update pw_workflow_runs
            set visible_at = @now,
                updated_at = @now
            where workflow_run_id = @workflow_run_id
              and status = @pending_status
              and lease_token is null
              and visible_at > @now
              and exists (
                  select 1
                  from pw_workflow_signal_waits wait
                  where wait.workflow_run_id = pw_workflow_runs.workflow_run_id
                    and wait.signal_name = @signal_name
                    and wait.completed_at is null);
            """;

        await using (var wakeCommand = new NpgsqlCommand(wakeSql, connection, transaction))
        {
            wakeCommand.Parameters.AddWithValue("workflow_run_id", workflowRunId);
            wakeCommand.Parameters.AddWithValue("signal_name", signalName);
            wakeCommand.Parameters.AddWithValue("pending_status", PendingStatus);
            wakeCommand.Parameters.AddWithValue("now", now);

            await wakeCommand.ExecuteNonQueryAsync(cancellationToken);
        }

        await transaction.CommitAsync(cancellationToken);
        return (Guid)inserted;
    }

    public async ValueTask<WorkflowStep?> GetStepAsync(
        Guid workflowRunId,
        int stepSequence,
        CancellationToken cancellationToken = default
    )
    {
        const string sql = """
            select
                workflow_run_id,
                step_seq,
                activity_name,
                activity_job_id,
                input,
                status,
                created_at,
                updated_at,
                completed_at,
                result,
                error
            from pw_workflow_steps
            where workflow_run_id = @workflow_run_id
              and step_seq = @step_seq;
            """;

        await using var command = _dataSource.CreateCommand(sql);
        command.Parameters.AddWithValue("workflow_run_id", workflowRunId);
        command.Parameters.AddWithValue("step_seq", stepSequence);

        await using var reader = await command.ExecuteReaderAsync(cancellationToken);
        if (!await reader.ReadAsync(cancellationToken))
        {
            return null;
        }

        return new WorkflowStep(
            reader.GetGuid(0),
            reader.GetInt32(1),
            reader.GetString(2),
            reader.GetGuid(3),
            ReadNullableString(reader, 4),
            ReadStepStatus(reader.GetString(5)),
            reader.GetFieldValue<DateTimeOffset>(6),
            reader.GetFieldValue<DateTimeOffset>(7),
            ReadNullableDateTimeOffset(reader, 8),
            ReadNullableString(reader, 9),
            ReadNullableString(reader, 10)
        );
    }

    public async ValueTask RecordStepScheduledAsync(
        Guid workflowRunId,
        int stepSequence,
        string activityName,
        Guid activityJobId,
        string? inputJson,
        CancellationToken cancellationToken = default
    )
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(activityName);

        var now = DateTimeOffset.UtcNow;

        const string sql = """
            insert into pw_workflow_steps (
                workflow_run_id,
                step_seq,
                activity_name,
                activity_job_id,
                input,
                status,
                created_at,
                updated_at,
                completed_at,
                result,
                error)
            values (
                @workflow_run_id,
                @step_seq,
                @activity_name,
                @activity_job_id,
                @input,
                @status,
                @created_at,
                @updated_at,
                null,
                null,
                null)
            on conflict (workflow_run_id, step_seq) do nothing;
            """;

        await using var command = _dataSource.CreateCommand(sql);
        command.Parameters.AddWithValue("workflow_run_id", workflowRunId);
        command.Parameters.AddWithValue("step_seq", stepSequence);
        command.Parameters.AddWithValue("activity_name", activityName);
        command.Parameters.AddWithValue("activity_job_id", activityJobId);
        command.Parameters.AddWithValue(
            "input",
            NpgsqlDbType.Jsonb,
            (object?)inputJson ?? DBNull.Value
        );
        command.Parameters.AddWithValue("status", ScheduledStatus);
        command.Parameters.AddWithValue("created_at", now);
        command.Parameters.AddWithValue("updated_at", now);

        await command.ExecuteNonQueryAsync(cancellationToken);
    }

    public ValueTask RecordStepSuccessAsync(
        Guid workflowRunId,
        int stepSequence,
        string? resultJson,
        CancellationToken cancellationToken = default
    ) =>
        UpdateStepAsync(
            workflowRunId,
            stepSequence,
            SucceededStatus,
            resultJson,
            error: null,
            completedAt: DateTimeOffset.UtcNow,
            cancellationToken
        );

    public ValueTask RecordStepFailureAsync(
        Guid workflowRunId,
        int stepSequence,
        string error,
        CancellationToken cancellationToken = default
    )
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(error);

        return UpdateStepAsync(
            workflowRunId,
            stepSequence,
            FailedStatus,
            resultJson: null,
            error,
            completedAt: DateTimeOffset.UtcNow,
            cancellationToken
        );
    }

    public async ValueTask<IReadOnlyList<WorkflowFailureHook>> ListFailureHooksAsync(
        Guid workflowRunId,
        CancellationToken cancellationToken = default
    )
    {
        const string sql = """
            select
                workflow_run_id,
                hook_seq,
                activity_name,
                activity_job_id,
                input,
                status,
                created_at,
                updated_at,
                completed_at,
                result,
                error
            from pw_workflow_failure_hooks
            where workflow_run_id = @workflow_run_id
            order by hook_seq desc;
            """;

        await using var command = _dataSource.CreateCommand(sql);
        command.Parameters.AddWithValue("workflow_run_id", workflowRunId);

        var hooks = new List<WorkflowFailureHook>();
        await using var reader = await command.ExecuteReaderAsync(cancellationToken);
        while (await reader.ReadAsync(cancellationToken))
        {
            hooks.Add(ReadFailureHook(reader));
        }

        return hooks;
    }

    public async ValueTask RecordFailureHookRegisteredAsync(
        Guid workflowRunId,
        int hookSequence,
        string activityName,
        string? inputJson,
        CancellationToken cancellationToken = default
    )
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(activityName);

        var now = DateTimeOffset.UtcNow;

        const string sql = """
            insert into pw_workflow_failure_hooks (
                workflow_run_id,
                hook_seq,
                activity_name,
                activity_job_id,
                input,
                status,
                created_at,
                updated_at,
                completed_at,
                result,
                error)
            values (
                @workflow_run_id,
                @hook_seq,
                @activity_name,
                null,
                @input,
                @status,
                @created_at,
                @updated_at,
                null,
                null,
                null)
            on conflict (workflow_run_id, hook_seq) do nothing;
            """;

        await using var command = _dataSource.CreateCommand(sql);
        command.Parameters.AddWithValue("workflow_run_id", workflowRunId);
        command.Parameters.AddWithValue("hook_seq", hookSequence);
        command.Parameters.AddWithValue("activity_name", activityName);
        command.Parameters.AddWithValue(
            "input",
            NpgsqlDbType.Jsonb,
            (object?)inputJson ?? DBNull.Value
        );
        command.Parameters.AddWithValue("status", RegisteredStatus);
        command.Parameters.AddWithValue("created_at", now);
        command.Parameters.AddWithValue("updated_at", now);

        await command.ExecuteNonQueryAsync(cancellationToken);
    }

    public async ValueTask RecordFailureHookScheduledAsync(
        Guid workflowRunId,
        int hookSequence,
        Guid activityJobId,
        CancellationToken cancellationToken = default
    )
    {
        const string sql = """
            update pw_workflow_failure_hooks
            set activity_job_id = coalesce(activity_job_id, @activity_job_id),
                status = @scheduled_status,
                updated_at = @updated_at,
                error = null
            where workflow_run_id = @workflow_run_id
              and hook_seq = @hook_seq
              and status in (@registered_status, @scheduled_status);
            """;

        await using var command = _dataSource.CreateCommand(sql);
        command.Parameters.AddWithValue("workflow_run_id", workflowRunId);
        command.Parameters.AddWithValue("hook_seq", hookSequence);
        command.Parameters.AddWithValue("activity_job_id", activityJobId);
        command.Parameters.AddWithValue("registered_status", RegisteredStatus);
        command.Parameters.AddWithValue("scheduled_status", ScheduledStatus);
        command.Parameters.AddWithValue("updated_at", DateTimeOffset.UtcNow);

        await command.ExecuteNonQueryAsync(cancellationToken);
    }

    public ValueTask RecordFailureHookSuccessAsync(
        Guid workflowRunId,
        int hookSequence,
        string? resultJson,
        CancellationToken cancellationToken = default
    ) =>
        UpdateFailureHookAsync(
            workflowRunId,
            hookSequence,
            SucceededStatus,
            resultJson,
            error: null,
            completedAt: DateTimeOffset.UtcNow,
            cancellationToken
        );

    public ValueTask RecordFailureHookFailureAsync(
        Guid workflowRunId,
        int hookSequence,
        string error,
        CancellationToken cancellationToken = default
    )
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(error);

        return UpdateFailureHookAsync(
            workflowRunId,
            hookSequence,
            FailedStatus,
            resultJson: null,
            error,
            completedAt: DateTimeOffset.UtcNow,
            cancellationToken
        );
    }

    private async ValueTask<bool> UpdateLeasedRunAsync(
        Guid workflowRunId,
        string leaseToken,
        string status,
        string? resultJson,
        string? error,
        DateTimeOffset? completedAt,
        CancellationToken cancellationToken
    )
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(leaseToken);

        const string sql = $"""
            update pw_workflow_runs
            set status = @status,
                updated_at = @updated_at,
                completed_at = @completed_at,
                result = @result,
                error = @error,
                {LeaseReleaseColumns}
            where workflow_run_id = @workflow_run_id
              and lease_token = @lease_token
              and status = @running_status;
            """;

        await using var command = _dataSource.CreateCommand(sql);
        command.Parameters.AddWithValue("workflow_run_id", workflowRunId);
        command.Parameters.AddWithValue("lease_token", leaseToken);
        command.Parameters.AddWithValue("running_status", RunningStatus);
        command.Parameters.AddWithValue("status", status);
        command.Parameters.AddWithValue("updated_at", DateTimeOffset.UtcNow);
        command.Parameters.AddWithValue("completed_at", (object?)completedAt ?? DBNull.Value);
        command.Parameters.AddWithValue(
            "result",
            NpgsqlDbType.Jsonb,
            (object?)resultJson ?? DBNull.Value
        );
        command.Parameters.AddWithValue("error", (object?)error ?? DBNull.Value);

        return await command.ExecuteNonQueryAsync(cancellationToken) == 1;
    }

    private async ValueTask UpdateStepAsync(
        Guid workflowRunId,
        int stepSequence,
        string status,
        string? resultJson,
        string? error,
        DateTimeOffset? completedAt,
        CancellationToken cancellationToken
    )
    {
        const string sql = """
            update pw_workflow_steps
            set status = @status,
                updated_at = @updated_at,
                completed_at = @completed_at,
                result = @result,
                error = @error
            where workflow_run_id = @workflow_run_id
              and step_seq = @step_seq;
            """;

        await using var command = _dataSource.CreateCommand(sql);
        command.Parameters.AddWithValue("workflow_run_id", workflowRunId);
        command.Parameters.AddWithValue("step_seq", stepSequence);
        command.Parameters.AddWithValue("status", status);
        command.Parameters.AddWithValue("updated_at", DateTimeOffset.UtcNow);
        command.Parameters.AddWithValue("completed_at", (object?)completedAt ?? DBNull.Value);
        command.Parameters.AddWithValue(
            "result",
            NpgsqlDbType.Jsonb,
            (object?)resultJson ?? DBNull.Value
        );
        command.Parameters.AddWithValue("error", (object?)error ?? DBNull.Value);

        await command.ExecuteNonQueryAsync(cancellationToken);
    }

    private async ValueTask UpdateFailureHookAsync(
        Guid workflowRunId,
        int hookSequence,
        string status,
        string? resultJson,
        string? error,
        DateTimeOffset? completedAt,
        CancellationToken cancellationToken
    )
    {
        const string sql = """
            update pw_workflow_failure_hooks
            set status = @status,
                updated_at = @updated_at,
                completed_at = @completed_at,
                result = @result,
                error = @error
            where workflow_run_id = @workflow_run_id
              and hook_seq = @hook_seq;
            """;

        await using var command = _dataSource.CreateCommand(sql);
        command.Parameters.AddWithValue("workflow_run_id", workflowRunId);
        command.Parameters.AddWithValue("hook_seq", hookSequence);
        command.Parameters.AddWithValue("status", status);
        command.Parameters.AddWithValue("updated_at", DateTimeOffset.UtcNow);
        command.Parameters.AddWithValue("completed_at", (object?)completedAt ?? DBNull.Value);
        command.Parameters.AddWithValue(
            "result",
            NpgsqlDbType.Jsonb,
            (object?)resultJson ?? DBNull.Value
        );
        command.Parameters.AddWithValue("error", (object?)error ?? DBNull.Value);

        await command.ExecuteNonQueryAsync(cancellationToken);
    }

    private static async ValueTask CompleteSignalWaitAsync(
        NpgsqlConnection connection,
        NpgsqlTransaction transaction,
        Guid workflowRunId,
        int waitSequence,
        DateTimeOffset completedAt,
        CancellationToken cancellationToken
    )
    {
        const string sql = """
            update pw_workflow_signal_waits
            set completed_at = coalesce(completed_at, @completed_at)
            where workflow_run_id = @workflow_run_id
              and wait_seq = @wait_seq;
            """;

        await using var command = new NpgsqlCommand(sql, connection, transaction);
        command.Parameters.AddWithValue("workflow_run_id", workflowRunId);
        command.Parameters.AddWithValue("wait_seq", waitSequence);
        command.Parameters.AddWithValue("completed_at", completedAt);

        await command.ExecuteNonQueryAsync(cancellationToken);
    }

    private static string? ReadNullableString(NpgsqlDataReader reader, int ordinal) =>
        reader.IsDBNull(ordinal) ? null : reader.GetString(ordinal);

    private static Guid? ReadNullableGuid(NpgsqlDataReader reader, int ordinal) =>
        reader.IsDBNull(ordinal) ? null : reader.GetGuid(ordinal);

    private static DateTimeOffset? ReadNullableDateTimeOffset(
        NpgsqlDataReader reader,
        int ordinal
    ) => reader.IsDBNull(ordinal) ? null : reader.GetFieldValue<DateTimeOffset>(ordinal);

    private static WorkflowFailureHook ReadFailureHook(NpgsqlDataReader reader) =>
        new(
            reader.GetGuid(0),
            reader.GetInt32(1),
            reader.GetString(2),
            ReadNullableGuid(reader, 3),
            ReadNullableString(reader, 4),
            ReadFailureHookStatus(reader.GetString(5)),
            reader.GetFieldValue<DateTimeOffset>(6),
            reader.GetFieldValue<DateTimeOffset>(7),
            ReadNullableDateTimeOffset(reader, 8),
            ReadNullableString(reader, 9),
            ReadNullableString(reader, 10)
        );

    private static LeasedWorkflowRun ReadLeasedWorkflowRun(NpgsqlDataReader reader) =>
        new(
            reader.GetGuid(0),
            reader.GetString(1),
            reader.GetInt32(2),
            reader.GetInt32(3),
            reader.GetString(4),
            reader.GetFieldValue<DateTimeOffset>(5)
        );

    private static WorkflowStatus ReadRunStatus(string value) =>
        value switch
        {
            PendingStatus => WorkflowStatus.Pending,
            RunningStatus => WorkflowStatus.Running,
            SucceededStatus => WorkflowStatus.Succeeded,
            FailedStatus => WorkflowStatus.Failed,
            _ => throw new InvalidOperationException($"Unknown workflow status '{value}'."),
        };

    private static WorkflowStepStatus ReadStepStatus(string value) =>
        value switch
        {
            ScheduledStatus => WorkflowStepStatus.Scheduled,
            SucceededStatus => WorkflowStepStatus.Succeeded,
            FailedStatus => WorkflowStepStatus.Failed,
            _ => throw new InvalidOperationException($"Unknown workflow step status '{value}'."),
        };

    private static WorkflowFailureHookStatus ReadFailureHookStatus(string value) =>
        value switch
        {
            RegisteredStatus => WorkflowFailureHookStatus.Registered,
            ScheduledStatus => WorkflowFailureHookStatus.Scheduled,
            SucceededStatus => WorkflowFailureHookStatus.Succeeded,
            FailedStatus => WorkflowFailureHookStatus.Failed,
            _ => throw new InvalidOperationException(
                $"Unknown workflow failure hook status '{value}'."
            ),
        };

    private const string LeaseReleaseColumns =
        "workflow_worker_id = null, lease_token = null, lease_expires_at = null";

    /// <summary>
    /// Shared shape of every park: flip the run back to pending, roll back the attempt the lease
    /// charged (a park is not a failed attempt), clear completion state, and release the lease —
    /// guarded by the lease token so a lost lease writes nothing. Only the visible_at policy
    /// differs per wait kind.
    /// </summary>
    private static string BuildParkSql(string visibleAtExpression) =>
        $"""
            update pw_workflow_runs
            set status = @pending_status,
                visible_at = {visibleAtExpression},
                attempt = greatest(attempt - 1, 0),
                updated_at = @updated_at,
                completed_at = null,
                result = null,
                error = null,
                {LeaseReleaseColumns}
            where workflow_run_id = @workflow_run_id
              and lease_token = @lease_token
              and status = @running_status;
            """;

    private static readonly string SleepParkSql = BuildParkSql("@fire_at");

    private static readonly string SignalParkSql = BuildParkSql("@visible_at");

    private static readonly string ReleaseRunSql = BuildParkSql("@visible_at");

    // Park with the grace deadline (safety net against a missed wake); the run is normally woken
    // much sooner by the activity store's edge-trigger or the recheck in RecordRunWaitingAsync.
    private static readonly string ActivityWaitParkSql = BuildParkSql("@grace_deadline");

    private const string PendingStatus = "pending";
    private const string RunningStatus = "running";
    private const string RegisteredStatus = "registered";
    private const string ScheduledStatus = "scheduled";
    private const string SucceededStatus = "succeeded";
    private const string FailedStatus = "failed";
}
