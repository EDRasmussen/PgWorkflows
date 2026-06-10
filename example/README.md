# PgWorkflows example: API client + worker

The topology from [Workers & scaling](../docs/src/content/docs/workers-and-scaling.md):

```
Example.Workflows/   <- workflow + activity classes (shared class library)
Example.Api/         <- client: AddWorkflow + DisableWorkers (queues runs, returns 202)
Example.Worker/      <- worker: AddWorkflow + AddActivities (waits for work and executes it)
```

The API never executes workflows; it inserts a run and returns `202 Accepted` with the run id.
Workers lease runs from Postgres and execute them. Docker Compose runs **three** worker instances
(`worker-1`..`worker-3`, ids set via `PGWORKFLOWS_WORKER_ID`).

## Run with Docker (everything in one go)

From the repo root:

```sh
docker compose up -d --build
```

Then open the Scalar API reference at <http://localhost:8080/scalar>, fire a `POST /greetings` with
`{"name":"Emil"}`, and watch the workers do the work:

```sh
docker compose logs -f example-worker-1 example-worker-2 example-worker-3
```
