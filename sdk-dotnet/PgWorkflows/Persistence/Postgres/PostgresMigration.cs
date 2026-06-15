namespace PgWorkflows.Persistence.Postgres;

/// <summary>
/// One versioned schema migration, applied in version order and recorded in
/// <c>pw_schema_migrations</c>.
/// </summary>
/// <param name="Version">Sequential version number, starting at 1.</param>
/// <param name="Name">Short human-readable label, stored with the bookkeeping row.</param>
/// <param name="Sql">The DDL to execute. Runs inside a transaction.</param>
public sealed record PostgresMigration(int Version, string Name, string Sql);
