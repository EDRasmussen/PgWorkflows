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

        const string sql = """
            insert into pw_workflow_runs (
                workflow_run_id,
                workflow_name,
                idempotency_key,
                input,
                status,
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
            reader.GetFieldValue<DateTimeOffset>(5),
            reader.GetFieldValue<DateTimeOffset>(6),
            ReadNullableDateTimeOffset(reader, 7),
            ReadNullableString(reader, 8),
            ReadNullableString(reader, 9),
            ReadNullableString(reader, 2)
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
                where status = @pending_status
                   or (status = @running_status and lease_expires_at <= @now)
                   or (status = @running_status and lease_expires_at is null and updated_at <= @stale_unleased_before)
                order by created_at
                for update skip locked
                limit @limit
            )
            update pw_workflow_runs as runs
            set status = @running_status,
                updated_at = @now,
                workflow_worker_id = @worker_id,
                lease_token = @lease_token,
                lease_expires_at = @lease_expires_at
            from leased
            where runs.workflow_run_id = leased.workflow_run_id
            returning
                runs.workflow_run_id,
                runs.workflow_name,
                runs.idempotency_key,
                runs.input,
                runs.status,
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
    )
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(error);

        return UpdateLeasedRunAsync(
            workflowRunId,
            leaseToken,
            FailedStatus,
            resultJson: null,
            error,
            completedAt: DateTimeOffset.UtcNow,
            cancellationToken
        );
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

    private static string? ReadNullableString(NpgsqlDataReader reader, int ordinal) =>
        reader.IsDBNull(ordinal) ? null : reader.GetString(ordinal);

    private static DateTimeOffset? ReadNullableDateTimeOffset(NpgsqlDataReader reader, int ordinal) =>
        reader.IsDBNull(ordinal) ? null : reader.GetFieldValue<DateTimeOffset>(ordinal);

    private static LeasedWorkflowRun ReadLeasedWorkflowRun(NpgsqlDataReader reader) =>
        new(
            reader.GetGuid(0),
            reader.GetString(1),
            ReadNullableString(reader, 3),
            ReadRunStatus(reader.GetString(4)),
            reader.GetFieldValue<DateTimeOffset>(5),
            reader.GetFieldValue<DateTimeOffset>(6),
            ReadNullableDateTimeOffset(reader, 7),
            ReadNullableString(reader, 8),
            ReadNullableString(reader, 9),
            reader.GetString(10),
            reader.GetFieldValue<DateTimeOffset>(11),
            ReadNullableString(reader, 2)
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

    private const string PendingStatus = "pending";
    private const string RunningStatus = "running";
    private const string ScheduledStatus = "scheduled";
    private const string SucceededStatus = "succeeded";
    private const string FailedStatus = "failed";
}
