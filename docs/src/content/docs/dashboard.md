---
title: Dashboard
description: A read-only web UI for your PgWorkflows tables, shipped as a Docker image.
---

A read-only web UI over the PgWorkflows tables: a live run feed with status filters and
a per-run view of steps, timers, signal waits, and failure hooks. It talks straight to
your Postgres and needs nothing else.

## Run it

The image is published to GitHub Container Registry:

```sh
docker run -p 3000:3000 \
  -e DATABASE_URL=postgres://user:pass@host:5432/yourdb \
  ghcr.io/edrasmussen/pgworkflows-dashboard
```

Then open <http://localhost:3000>. `DATABASE_URL` is the only configuration. Point it at
the database your workers use. The dashboard only reads, so a read-only role works.

The repo's `docker compose up --build -d` starts it alongside the example stack.
