namespace PgWorkflows.Persistence.Postgres;

/// <summary>
/// The idempotent DDL PgWorkflows applies on startup (when <c>ensureSchemaOnStart</c> is true).
/// Exposed so deployments that run migrations themselves can apply it out-of-band instead.
/// </summary>
public static class PostgresSchema
{
    /// <summary>The full schema script; safe to re-run, serialized by an advisory lock on startup.</summary>
    public static readonly string Sql = """
        create table if not exists pw_activity_jobs (
            job_id uuid primary key,
            activity_name text not null,
            idempotency_key text null,
            input jsonb null,
            status text not null,
            attempt integer not null,
            max_attempts integer not null,
            created_at timestamptz not null,
            visible_at timestamptz not null,
            lease_token text null,
            lease_expires_at timestamptz null,
            completed_at timestamptz null,
            result jsonb null,
            error text null
        );

        alter table pw_activity_jobs
            add column if not exists idempotency_key text null;

        alter table pw_activity_jobs
            add column if not exists workflow_run_id uuid null;

        create index if not exists ix_pw_activity_jobs_runnable
            on pw_activity_jobs (visible_at)
            where status in ('pending', 'leased');

        -- Supports the "does this run still have incomplete activity jobs?" probe used to wake a
        -- parked run exactly once (when its last outstanding job completes).
        create index if not exists ix_pw_activity_jobs_run_incomplete
            on pw_activity_jobs (workflow_run_id)
            where status in ('pending', 'leased');

        create unique index if not exists ux_pw_activity_jobs_idempotency
            on pw_activity_jobs (activity_name, idempotency_key)
            where idempotency_key is not null;

        create table if not exists pw_workflow_runs (
            workflow_run_id uuid primary key,
            workflow_name text not null,
            idempotency_key text null,
            input jsonb null,
            status text not null,
            attempt integer not null,
            max_attempts integer not null,
            visible_at timestamptz not null,
            created_at timestamptz not null,
            updated_at timestamptz not null,
            completed_at timestamptz null,
            result jsonb null,
            error text null,
            workflow_worker_id text null,
            lease_token text null,
            lease_expires_at timestamptz null
        );

        alter table pw_workflow_runs
            add column if not exists idempotency_key text null;

        alter table pw_workflow_runs
            add column if not exists workflow_worker_id text null;

        alter table pw_workflow_runs
            add column if not exists lease_token text null;

        alter table pw_workflow_runs
            add column if not exists lease_expires_at timestamptz null;

        alter table pw_workflow_runs
            add column if not exists attempt integer not null default 0;

        alter table pw_workflow_runs
            add column if not exists max_attempts integer not null default 1;

        alter table pw_workflow_runs
            add column if not exists visible_at timestamptz not null default now();

        create unique index if not exists ux_pw_workflow_runs_idempotency
            on pw_workflow_runs (workflow_name, idempotency_key)
            where idempotency_key is not null;

        create index if not exists ix_pw_workflow_runs_runnable
            on pw_workflow_runs (visible_at, created_at)
            where status in ('pending', 'running');

        -- Observability queries (e.g. the dashboard): newest-first feed with keyset pagination,
        -- plus the same feed filtered by workflow name or status.
        create index if not exists ix_pw_workflow_runs_created
            on pw_workflow_runs (created_at desc, workflow_run_id desc);

        create index if not exists ix_pw_workflow_runs_name_created
            on pw_workflow_runs (workflow_name, created_at desc);

        create index if not exists ix_pw_workflow_runs_status_created
            on pw_workflow_runs (status, created_at desc);

        create table if not exists pw_workflow_steps (
            workflow_run_id uuid not null references pw_workflow_runs(workflow_run_id) on delete cascade,
            step_seq integer not null,
            activity_name text not null,
            activity_job_id uuid not null references pw_activity_jobs(job_id),
            input jsonb null,
            status text not null,
            created_at timestamptz not null,
            updated_at timestamptz not null,
            completed_at timestamptz null,
            result jsonb null,
            error text null,
            primary key (workflow_run_id, step_seq)
        );

        create unique index if not exists ux_pw_workflow_steps_activity_job
            on pw_workflow_steps (activity_job_id);

        create table if not exists pw_workflow_failure_hooks (
            workflow_run_id uuid not null references pw_workflow_runs(workflow_run_id) on delete cascade,
            hook_seq integer not null,
            activity_name text not null,
            activity_job_id uuid null references pw_activity_jobs(job_id),
            input jsonb null,
            status text not null,
            created_at timestamptz not null,
            updated_at timestamptz not null,
            completed_at timestamptz null,
            result jsonb null,
            error text null,
            primary key (workflow_run_id, hook_seq)
        );

        create unique index if not exists ux_pw_workflow_failure_hooks_activity_job
            on pw_workflow_failure_hooks (activity_job_id)
            where activity_job_id is not null;

        create table if not exists pw_workflow_timers (
            workflow_run_id uuid not null references pw_workflow_runs(workflow_run_id) on delete cascade,
            timer_seq integer not null,
            fire_at timestamptz not null,
            created_at timestamptz not null,
            primary key (workflow_run_id, timer_seq)
        );

        create table if not exists pw_workflow_signals (
            workflow_run_id uuid not null references pw_workflow_runs(workflow_run_id) on delete cascade,
            signal_seq bigint generated by default as identity,
            signal_id uuid not null,
            signal_name text not null,
            idempotency_key text null,
            payload jsonb not null,
            created_at timestamptz not null,
            consumed_by_wait_seq integer null,
            consumed_at timestamptz null,
            primary key (workflow_run_id, signal_seq)
        );

        create unique index if not exists ux_pw_workflow_signals_signal_id
            on pw_workflow_signals (signal_id);

        create unique index if not exists ux_pw_workflow_signals_idempotency
            on pw_workflow_signals (workflow_run_id, signal_name, idempotency_key)
            where idempotency_key is not null;

        create index if not exists ix_pw_workflow_signals_unconsumed
            on pw_workflow_signals (workflow_run_id, signal_name, signal_seq)
            where consumed_by_wait_seq is null;

        create index if not exists ix_pw_workflow_signals_consumed
            on pw_workflow_signals (workflow_run_id, signal_name, consumed_by_wait_seq)
            where consumed_by_wait_seq is not null;

        create table if not exists pw_workflow_signal_waits (
            workflow_run_id uuid not null references pw_workflow_runs(workflow_run_id) on delete cascade,
            wait_seq integer not null,
            signal_name text not null,
            created_at timestamptz not null,
            completed_at timestamptz null,
            primary key (workflow_run_id, wait_seq)
        );

        create index if not exists ix_pw_workflow_signal_waits_active
            on pw_workflow_signal_waits (workflow_run_id, signal_name)
            where completed_at is null;
        """;
}
