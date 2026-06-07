namespace PgWorkflows.Persistence.Postgres;

public static class PostgresSchema
{
    public const string Sql = """
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

        create index if not exists ix_pw_activity_jobs_runnable
            on pw_activity_jobs (visible_at)
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
            created_at timestamptz not null,
            updated_at timestamptz not null,
            completed_at timestamptz null,
            result jsonb null,
            error text null
        );

        alter table pw_workflow_runs
            add column if not exists idempotency_key text null;

        create unique index if not exists ux_pw_workflow_runs_idempotency
            on pw_workflow_runs (workflow_name, idempotency_key)
            where idempotency_key is not null;

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
        """;
}
