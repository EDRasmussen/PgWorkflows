# PgWorkflows dashboard

A read-only web UI for the PgWorkflows tables: live run feed with status filters and keyset
pagination, plus a per-run detail view of steps, timers, signal waits, and failure hooks. It
connects straight to your Postgres and needs nothing else.

## Run it

```sh
docker run -p 3000:3000 \
  -e DATABASE_URL=postgres://user:pass@host:5432/yourdb \
  ghcr.io/edrasmussen/pgworkflows-dashboard
```

Then open <http://localhost:3000>. `DATABASE_URL` is the only configuration; point it at the
database your workers use. The dashboard only reads, so a read-only role works.

## Develop

```sh
pnpm install
cp .env.example .env
pnpm dev
```

The repo's `docker compose up --build -d` starts Postgres, the example API and workers, and this
dashboard together.

After a schema change in the SDK, regenerate the typed table definitions:

```sh
pnpm db:codegen
```
