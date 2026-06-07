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

        create index if not exists ix_pw_activity_jobs_runnable
            on pw_activity_jobs (visible_at)
            where status in ('pending', 'leased');

        create unique index if not exists ux_pw_activity_jobs_idempotency
            on pw_activity_jobs (activity_name, idempotency_key)
            where idempotency_key is not null;
        """;
}
