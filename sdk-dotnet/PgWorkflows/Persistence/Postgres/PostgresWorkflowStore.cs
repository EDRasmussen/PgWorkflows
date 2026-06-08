using Npgsql;
using NpgsqlTypes;
using PgWorkflows.Workflows;

namespace PgWorkflows.Persistence.Postgres;

public sealed class PostgresWorkflowStore(NpgsqlDataSource dataSource) : IWorkflowStore
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
            throw new ArgumentException("Idempotency key cannot be empty or whitespace.", nameof(request));
        }

        var workflowRunId = Guid.NewGuid();
        var now = DateTimeOffset.UtcNow;
        var visibleAt = request.VisibleAt ?? now;

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
        command.Parameters.AddWithValue("max_attempts", Math.Max(request.MaxAttempts, 1));
        command.Parameters.AddWithValue("visible_at", visibleAt);
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
        LeaseWorkflowRunsRequest request,
        CancellationToken cancellationToken = default
    )
    {
        ArgumentNullException.ThrowIfNull(request);
        ArgumentException.ThrowIfNullOrWhiteSpace(request.WorkerId);

        var limit = Math.Max(request.Limit, 1);
        var leaseToken = Guid.NewGuid().ToString("N");
        var leaseExpiresAt = request.Now.Add(request.LeaseDuration);
        var staleUnleasedBefore = request.Now.Subtract(request.LeaseDuration);

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
                runs.idempotency_key,
                runs.input,
                runs.status,
                runs.attempt,
                runs.max_attempts,
                runs.visible_at,
                runs.created_at,
                runs.updated_at,
                runs.completed_at,
                runs.result,
                runs.error,
                runs.lease_token,
                runs.lease_expires_at;
            """;

        await using var command = _dataSource.CreateCommand(sql);
        command.Parameters.AddWithValue("pending_status", PendingStatus);
        command.Parameters.AddWithValue("running_status", RunningStatus);
        command.Parameters.AddWithValue("now", request.Now);
        command.Parameters.AddWithValue("stale_unleased_before", staleUnleasedBefore);
        command.Parameters.AddWithValue("limit", limit);
        command.Parameters.AddWithValue("max_attempts", Math.Max(request.MaxAttempts, 1));
        command.Parameters.AddWithValue("worker_id", request.WorkerId);
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

    public ValueTask MarkRunRunningAsync(
        Guid workflowRunId,
        CancellationToken cancellationToken = default
    ) =>
        UpdateRunAsync(
            workflowRunId,
            RunningStatus,
            resultJson: null,
            error: null,
            completedAt: null,
            cancellationToken
        );

    public async ValueTask<bool> RenewRunLeaseAsync(
        Guid workflowRunId,
        string leaseToken,
        DateTimeOffset leaseExpiresAt,
        CancellationToken cancellationToken = default
    )
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(leaseToken);

        const string sql = """
            update pw_workflow_runs
            set updated_at = @updated_at,
                lease_expires_at = @lease_expires_at
            where workflow_run_id = @workflow_run_id
              and lease_token = @lease_token
              and status = @running_status;
            """;

        await using var command = _dataSource.CreateCommand(sql);
        command.Parameters.AddWithValue("workflow_run_id", workflowRunId);
        command.Parameters.AddWithValue("lease_token", leaseToken);
        command.Parameters.AddWithValue("running_status", RunningStatus);
        command.Parameters.AddWithValue("updated_at", DateTimeOffset.UtcNow);
        command.Parameters.AddWithValue("lease_expires_at", leaseExpiresAt);

        return await command.ExecuteNonQueryAsync(cancellationToken) == 1;
    }

    public ValueTask RecordRunSuccessAsync(
        Guid workflowRunId,
        string? resultJson,
        CancellationToken cancellationToken = default
    ) =>
        UpdateRunAsync(
            workflowRunId,
            SucceededStatus,
            resultJson,
            error: null,
            completedAt: DateTimeOffset.UtcNow,
            cancellationToken
        );

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

    public ValueTask RecordRunFailureAsync(
        Guid workflowRunId,
        string error,
        CancellationToken cancellationToken = default
    )
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(error);

        return UpdateRunAsync(
            workflowRunId,
            FailedStatus,
            resultJson: null,
            error,
            completedAt: DateTimeOffset.UtcNow,
            cancellationToken
        );
    }

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

        const string sql = """
            update pw_workflow_runs
            set status = @status,
                updated_at = @updated_at,
                completed_at = @completed_at,
                result = null,
                error = @error,
                visible_at = @visible_at,
                workflow_worker_id = null,
                lease_token = null,
                lease_expires_at = null
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
        command.Parameters.AddWithValue(
            "completed_at",
            retryable ? DBNull.Value : now
        );
        command.Parameters.AddWithValue("error", error);
        command.Parameters.AddWithValue("visible_at", retryable ? nextVisibleAt ?? now : now);

        return await command.ExecuteNonQueryAsync(cancellationToken) == 1;
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
        RecordWorkflowStepRequest request,
        CancellationToken cancellationToken = default
    )
    {
        ArgumentNullException.ThrowIfNull(request);
        ArgumentException.ThrowIfNullOrWhiteSpace(request.ActivityName);

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
        command.Parameters.AddWithValue("workflow_run_id", request.WorkflowRunId);
        command.Parameters.AddWithValue("step_seq", request.StepSequence);
        command.Parameters.AddWithValue("activity_name", request.ActivityName);
        command.Parameters.AddWithValue("activity_job_id", request.ActivityJobId);
        command.Parameters.AddWithValue(
            "input",
            NpgsqlDbType.Jsonb,
            (object?)request.InputJson ?? DBNull.Value
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
        RecordWorkflowFailureHookRequest request,
        CancellationToken cancellationToken = default
    )
    {
        ArgumentNullException.ThrowIfNull(request);
        ArgumentException.ThrowIfNullOrWhiteSpace(request.ActivityName);

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
        command.Parameters.AddWithValue("workflow_run_id", request.WorkflowRunId);
        command.Parameters.AddWithValue("hook_seq", request.HookSequence);
        command.Parameters.AddWithValue("activity_name", request.ActivityName);
        command.Parameters.AddWithValue(
            "input",
            NpgsqlDbType.Jsonb,
            (object?)request.InputJson ?? DBNull.Value
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

    private async ValueTask UpdateRunAsync(
        Guid workflowRunId,
        string status,
        string? resultJson,
        string? error,
        DateTimeOffset? completedAt,
        CancellationToken cancellationToken
    )
    {
        const string sql = """
            update pw_workflow_runs
            set status = @status,
                updated_at = @updated_at,
                completed_at = @completed_at,
                result = @result,
                error = @error,
                workflow_worker_id = null,
                lease_token = null,
                lease_expires_at = null
            where workflow_run_id = @workflow_run_id;
            """;

        await using var command = _dataSource.CreateCommand(sql);
        command.Parameters.AddWithValue("workflow_run_id", workflowRunId);
        command.Parameters.AddWithValue("status", status);
        command.Parameters.AddWithValue("updated_at", DateTimeOffset.UtcNow);
        command.Parameters.AddWithValue("completed_at", (object?)completedAt ?? DBNull.Value);
        command.Parameters.AddWithValue("result", NpgsqlDbType.Jsonb, (object?)resultJson ?? DBNull.Value);
        command.Parameters.AddWithValue("error", (object?)error ?? DBNull.Value);

        await command.ExecuteNonQueryAsync(cancellationToken);
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

        const string sql = """
            update pw_workflow_runs
            set status = @status,
                updated_at = @updated_at,
                completed_at = @completed_at,
                result = @result,
                error = @error,
                workflow_worker_id = null,
                lease_token = null,
                lease_expires_at = null
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
        command.Parameters.AddWithValue("result", NpgsqlDbType.Jsonb, (object?)resultJson ?? DBNull.Value);
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
        command.Parameters.AddWithValue("result", NpgsqlDbType.Jsonb, (object?)resultJson ?? DBNull.Value);
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
        command.Parameters.AddWithValue("result", NpgsqlDbType.Jsonb, (object?)resultJson ?? DBNull.Value);
        command.Parameters.AddWithValue("error", (object?)error ?? DBNull.Value);

        await command.ExecuteNonQueryAsync(cancellationToken);
    }

    private static string? ReadNullableString(NpgsqlDataReader reader, int ordinal) =>
        reader.IsDBNull(ordinal) ? null : reader.GetString(ordinal);

    private static Guid? ReadNullableGuid(NpgsqlDataReader reader, int ordinal) =>
        reader.IsDBNull(ordinal) ? null : reader.GetGuid(ordinal);

    private static DateTimeOffset? ReadNullableDateTimeOffset(NpgsqlDataReader reader, int ordinal) =>
        reader.IsDBNull(ordinal) ? null : reader.GetFieldValue<DateTimeOffset>(ordinal);

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
            ReadNullableString(reader, 3),
            ReadRunStatus(reader.GetString(4)),
            reader.GetFieldValue<DateTimeOffset>(8),
            reader.GetFieldValue<DateTimeOffset>(9),
            ReadNullableDateTimeOffset(reader, 10),
            ReadNullableString(reader, 11),
            ReadNullableString(reader, 12),
            reader.GetString(13),
            reader.GetFieldValue<DateTimeOffset>(14),
            ReadNullableString(reader, 2),
            reader.GetInt32(5),
            reader.GetInt32(6),
            reader.GetFieldValue<DateTimeOffset>(7)
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
            _ => throw new InvalidOperationException($"Unknown workflow failure hook status '{value}'."),
        };

    private const string PendingStatus = "pending";
    private const string RunningStatus = "running";
    private const string RegisteredStatus = "registered";
    private const string ScheduledStatus = "scheduled";
    private const string SucceededStatus = "succeeded";
    private const string FailedStatus = "failed";
}
