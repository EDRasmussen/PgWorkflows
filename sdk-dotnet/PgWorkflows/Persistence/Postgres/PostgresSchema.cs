namespace PgWorkflows.Persistence.Postgres;

public static class PostgresSchema
{
    public const string Sql = """
        create table if not exists pw_activity_jobs (
            job_id uuid primary key,
            activity_name text not null,
            input text null,
            status text not null,
            attempt integer not null,
            max_attempts integer not null,
            created_at timestamptz not null,
            visible_at timestamptz not null,
            lease_token text null,
            lease_expires_at timestamptz null,
            completed_at timestamptz null,
            result text null,
            error text null
        );

        create index if not exists ix_pw_activity_jobs_runnable
            on pw_activity_jobs (visible_at)
            where status in ('pending', 'leased');
        """;
}
