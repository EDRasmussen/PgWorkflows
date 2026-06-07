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
                error = @error
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
