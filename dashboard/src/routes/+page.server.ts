import { getDb } from "$lib/server/db";
import { sql } from "kysely";
import type { PageServerLoad } from "./$types";

const PAGE_SIZE = 50;
const UUID_RE = /^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$/i;
const STATUSES = ["pending", "running", "succeeded", "failed"];

// Keyset cursor: keep ms precision (ts) + id
type Cursor = { ts: string; id: string };

const parseCursor = (raw: string | null): Cursor | null => {
  if (!raw) return null;
  const split = raw.lastIndexOf("_");
  if (split < 1) return null;
  const ts = raw.slice(0, split);
  const id = raw.slice(split + 1);
  return UUID_RE.test(id) ? { ts, id } : null;
};

const loadRunDetail = async (runId: string) => {
  const db = getDb();
  const run = await db
    .selectFrom("pw_workflow_runs")
    .selectAll()
    .where("workflow_run_id", "=", runId)
    .executeTakeFirst();

  if (!run) return null;

  const [steps, failureHooks, signalWaits, timers] = await Promise.all([
    db
      .selectFrom("pw_workflow_steps")
      .selectAll()
      .where("workflow_run_id", "=", runId)
      .orderBy("step_seq")
      .execute(),
    db
      .selectFrom("pw_workflow_failure_hooks")
      .selectAll()
      .where("workflow_run_id", "=", runId)
      .orderBy("hook_seq")
      .execute(),
    db
      .selectFrom("pw_workflow_signal_waits")
      .select(["signal_name", "created_at"])
      .where("workflow_run_id", "=", runId)
      .where("completed_at", "is", null)
      .execute(),
    db
      .selectFrom("pw_workflow_timers")
      .select(["fire_at"])
      .where("workflow_run_id", "=", runId)
      .where("fire_at", ">", sql<Date>`now()`)
      .orderBy("fire_at")
      .execute(),
  ]);

  return { run, steps, failureHooks, signalWaits, timers };
};

export const load: PageServerLoad = async ({ url }) => {
  const db = getDb();
  const runId = url.searchParams.get("run");

  const statusParam = url.searchParams.get("status");
  const status = statusParam && STATUSES.includes(statusParam) ? statusParam : null;
  const workflow = url.searchParams.get("workflow");
  const q = url.searchParams.get("q")?.trim() ?? "";
  const dir = url.searchParams.get("dir") === "asc" ? "asc" : "desc";
  const after = parseCursor(url.searchParams.get("after"));
  const before = after ? null : parseCursor(url.searchParams.get("before"));

  let runsQuery = db.selectFrom("pw_workflow_runs as r").select((eb) => [
    "r.workflow_run_id",
    "r.workflow_name",
    "r.status",
    "r.attempt",
    "r.max_attempts",
    "r.created_at",
    "r.completed_at",
    sql<string>`r.created_at::text`.as("cursor_ts"),
    eb
      .selectFrom("pw_workflow_steps as s")
      .whereRef("s.workflow_run_id", "=", "r.workflow_run_id")
      .select((seb) => seb.fn.countAll<number>().as("c"))
      .as("steps_total"),
    eb
      .selectFrom("pw_workflow_steps as s")
      .whereRef("s.workflow_run_id", "=", "r.workflow_run_id")
      .where("s.status", "=", "succeeded")
      .select((seb) => seb.fn.countAll<number>().as("c"))
      .as("steps_done"),
    eb
      .selectFrom("pw_workflow_timers as t")
      .whereRef("t.workflow_run_id", "=", "r.workflow_run_id")
      .where("t.fire_at", ">", sql<Date>`now()`)
      .select((teb) => teb.fn.max("t.fire_at").as("f"))
      .as("sleeping_until"),
    eb
      .selectFrom("pw_workflow_signal_waits as w")
      .whereRef("w.workflow_run_id", "=", "r.workflow_run_id")
      .where("w.completed_at", "is", null)
      .select("w.signal_name")
      .orderBy("w.wait_seq")
      .limit(1)
      .as("waiting_for_signal"),
  ]);

  if (status) runsQuery = runsQuery.where("r.status", "=", status);
  if (workflow) runsQuery = runsQuery.where("r.workflow_name", "=", workflow);
  if (q) {
    runsQuery = UUID_RE.test(q)
      ? runsQuery.where("r.workflow_run_id", "=", q)
      : runsQuery.where(sql<boolean>`false`);
  }

  const cursor = after ?? before;
  if (cursor) {
    const op = (dir === "desc") === (after !== null) ? "<" : ">";
    runsQuery = runsQuery.where((eb) =>
      eb(
        eb.refTuple("r.created_at", "r.workflow_run_id"),
        op,
        eb.tuple(sql<Date>`${cursor.ts}::timestamptz`, sql<string>`${cursor.id}::uuid`),
      ),
    );
  }

  const order = before ? (dir === "desc" ? "asc" : "desc") : dir;

  const [statusCounts, workflowNames, rowsRaw, selected] = await Promise.all([
    db
      .selectFrom("pw_workflow_runs")
      .select(["status", (eb) => eb.fn.countAll<number>().as("count")])
      .where("created_at", ">", sql<Date>`now() - interval '24 hours'`)
      .groupBy("status")
      .orderBy("status")
      .execute(),
    db
      .selectFrom("pw_workflow_runs")
      .select("workflow_name")
      .distinct()
      .orderBy("workflow_name")
      .execute(),
    runsQuery
      .orderBy("r.created_at", order)
      .orderBy("r.workflow_run_id", order)
      .limit(PAGE_SIZE + 1)
      .execute(),
    runId && UUID_RE.test(runId) ? loadRunDetail(runId) : null,
  ]);

  const hasMore = rowsRaw.length > PAGE_SIZE;
  const rows = rowsRaw.slice(0, PAGE_SIZE);
  if (before) rows.reverse();

  const cursorOf = (row: (typeof rows)[number]) => `${row.cursor_ts}_${row.workflow_run_id}`;

  return {
    statusCounts,
    workflows: workflowNames.map((w) => w.workflow_name),
    runs: rows.map((r) => ({
      ...r,
      steps_total: r.steps_total ?? 0,
      steps_done: r.steps_done ?? 0,
    })),
    filters: { status, workflow, q, dir },
    page: {
      hasPrev: before ? hasMore : cursor !== null,
      hasNext: before ? true : hasMore,
      prevCursor: rows.length > 0 ? cursorOf(rows[0]) : null,
      nextCursor: rows.length > 0 ? cursorOf(rows[rows.length - 1]) : null,
    },
    selected,
  };
};
