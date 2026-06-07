using System.Collections.Concurrent;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using PgWorkflows;
using PgWorkflows.Activities;
using PgWorkflows.Jobs;
using PgWorkflows.Persistence;
using PgWorkflows.Persistence.Postgres;
using PgWorkflows.Workers;
using Xunit;

namespace PgWorkflows.Tests;

/// <summary>
/// End-to-end behaviour of the worker against real Postgres. Each test drives the full
/// public path (enqueue → lease → execute → terminal state) so they pin behaviour a
/// single SQL statement does not obviously guarantee.
/// </summary>
public sealed class WorkerTests(PostgresFixture fixture) : PostgresTestBase(fixture)
{
    [Fact]
    public async Task Worker_runs_activity_and_persists_result()
    {
        var registry = new ActivityRegistry();
        registry.Register("greet", static (string input) => $"hello {input}");
        var jobId = await Store.EnqueueAsync("greet", "world");
        var worker = new ActivityWorker(registry, Store, Options("w1"));

        var processed = await worker.RunOnceAsync();

        Assert.Equal(1, processed);
        var job = await Store.GetAsync(jobId);
        Assert.NotNull(job);
        Assert.Equal(JobStatus.Succeeded, job!.Status);
        Assert.Equal("hello world", job.GetResult<string>());
        Assert.Equal(1, job.Attempt);
    }

    [Fact]
    public async Task Worker_runs_typed_activity_and_persists_typed_result()
    {
        var registry = new ActivityRegistry();
        registry.Register("typed-greet", static (GreetingInput input) => new GreetingOutput($"hello {input.Name}"));
        registry.Register("uppercase", static (GreetingInput input) => new GreetingActivities().Uppercase(input));
        var greetingId = await Store.EnqueueAsync("typed-greet", new GreetingInput("world"));
        var uppercaseId = await Store.EnqueueAsync("uppercase", new GreetingInput("world"));
        var worker = new ActivityWorker(registry, Store, Options("w1"));

        var firstBatch = await worker.RunOnceAsync();
        var secondBatch = await worker.RunOnceAsync();

        Assert.Equal(2, firstBatch + secondBatch);
        var greeting = await Store.GetAsync(greetingId);
        var uppercase = await Store.GetAsync(uppercaseId);
        Assert.NotNull(greeting);
        Assert.NotNull(uppercase);
        Assert.Equal(JobStatus.Succeeded, greeting!.Status);
        Assert.Equal(JobStatus.Succeeded, uppercase!.Status);
        Assert.Equal(new GreetingOutput("hello world"), greeting.GetResult<GreetingOutput>());
        Assert.Equal(new GreetingOutput("WORLD"), uppercase.GetResult<GreetingOutput>());
    }

    [Fact]
    public async Task Enqueue_with_same_activity_and_idempotency_key_returns_existing_job()
    {
        var first = await Store.EnqueueAsync("dedupe", "first", idempotencyKey: "order-1");
        var second = await Store.EnqueueAsync("dedupe", "second", idempotencyKey: "order-1");

        Assert.Equal(first, second);
        Assert.Equal(1, await CountJobsAsync("dedupe", "order-1"));
        var job = await Store.GetAsync(first);
        Assert.Equal("order-1", job!.IdempotencyKey);
    }

    [Fact]
    public async Task Duplicate_idempotent_enqueue_runs_activity_once()
    {
        var executions = 0;
        var registry = new ActivityRegistry();
        registry.Register<string, string>(
            "dedupe-run",
            input =>
            {
                Interlocked.Increment(ref executions);
                return input;
            }
        );
        var first = await Store.EnqueueAsync("dedupe-run", "first", idempotencyKey: "order-2");
        var second = await Store.EnqueueAsync("dedupe-run", "second", idempotencyKey: "order-2");
        var worker = new ActivityWorker(registry, Store, Options("w1"));

        var firstBatch = await worker.RunOnceAsync();
        var secondBatch = await worker.RunOnceAsync();

        Assert.Equal(first, second);
        Assert.Equal(1, firstBatch + secondBatch);
        Assert.Equal(1, executions);
        var job = await Store.GetAsync(first);
        Assert.Equal(JobStatus.Succeeded, job!.Status);
        Assert.Equal("first", job.GetResult<string>());
    }

    [Fact]
    public async Task Same_idempotency_key_is_scoped_by_activity_name()
    {
        var first = await Store.EnqueueAsync("dedupe-a", "input", idempotencyKey: "shared-key");
        var second = await Store.EnqueueAsync("dedupe-b", "input", idempotencyKey: "shared-key");

        Assert.NotEqual(first, second);
        Assert.Equal(1, await CountJobsAsync("dedupe-a", "shared-key"));
        Assert.Equal(1, await CountJobsAsync("dedupe-b", "shared-key"));
    }

    [Fact]
    public async Task Null_idempotency_key_does_not_dedupe()
    {
        var first = await Store.EnqueueAsync("no-dedupe", "first");
        var second = await Store.EnqueueAsync("no-dedupe", "second");

        Assert.NotEqual(first, second);
        Assert.Equal(2, await CountJobsAsync("no-dedupe"));
    }

    [Fact]
    public async Task Concurrent_enqueues_with_same_idempotency_key_create_one_job()
    {
        var ids = await Task.WhenAll(
            Enumerable.Range(0, 32)
                .Select(i => Store.EnqueueAsync("dedupe-concurrent", i, idempotencyKey: "order-3").AsTask())
        );

        var id = Assert.Single(ids.Distinct());
        Assert.NotEqual(Guid.Empty, id);
        Assert.Equal(1, await CountJobsAsync("dedupe-concurrent", "order-3"));
    }

    [Fact]
    public async Task Hosted_worker_registered_with_AddPgWorkflows_processes_activity()
    {
        var services = new ServiceCollection();
        services.AddSingleton<IActivityJobStore>(Store);
        services.AddSingleton(new GreetingPrefix("hello"));
        services.AddPgWorkflows(pg =>
            pg.ConfigureWorker(options =>
                    options with
                    {
                        WorkerId = "hosted-worker",
                        BatchSize = 1,
                        PollInterval = TimeSpan.FromMilliseconds(50),
                    }
                )
                .AddActivities<HostedGreetingActivities>()
        );

        await using var provider = services.BuildServiceProvider();
        var hostedService = Assert.Single(provider.GetServices<IHostedService>());
        await hostedService.StartAsync(CancellationToken.None);

        try
        {
            var greetId = await Store.EnqueueAsync("hosted-greet", "world");
            var repeatId = await Store.EnqueueAsync("hosted-repeat", 2);
            var greet = await WaitForTerminalAsync(greetId, TimeSpan.FromSeconds(10));
            var repeat = await WaitForTerminalAsync(repeatId, TimeSpan.FromSeconds(10));

            Assert.Equal(JobStatus.Succeeded, greet.Status);
            Assert.Equal(JobStatus.Succeeded, repeat.Status);
            Assert.Equal("hello world", greet.GetResult<string>());
            Assert.Equal("hello hello", repeat.GetResult<string>());
        }
        finally
        {
            await hostedService.StopAsync(CancellationToken.None);
        }
    }

    [Fact]
    public async Task Idle_worker_wakes_when_activity_is_enqueued()
    {
        var registry = new ActivityRegistry();
        registry.Register("notify-greet", static (string input) => $"hello {input}");
        await using var wakeup = new PostgresActivityJobWakeup(DataSource);
        var worker = new ActivityWorker(
            registry,
            Store,
            Options("notify-worker") with { PollInterval = TimeSpan.FromSeconds(30) },
            wakeup: wakeup
        );

        using var cts = new CancellationTokenSource();
        var run = worker.RunAsync(cts.Token);

        try
        {
            await Task.Delay(500);
            var jobId = await Store.EnqueueAsync("notify-greet", "world");
            var job = await WaitForTerminalAsync(jobId, TimeSpan.FromSeconds(3));

            Assert.Equal(JobStatus.Succeeded, job.Status);
            Assert.Equal("hello world", job.GetResult<string>());
        }
        finally
        {
            cts.Cancel();
            await SwallowCancellation(run);
        }
    }

    [Fact]
    public async Task Failing_activity_retries_until_max_attempts_then_fails_terminally()
    {
        var executions = 0;
        var registry = new ActivityRegistry();
        registry.Register<object?, string>(
            "boom",
            (_, _, _) =>
            {
                Interlocked.Increment(ref executions);
                throw new InvalidOperationException("boom");
            }
        );
        var jobId = await Store.EnqueueAsync("boom", (object?)null, maxAttempts: 3);
        var worker = new ActivityWorker(
            registry,
            Store,
            Options("w1") with { GetRetryDelay = static _ => TimeSpan.Zero }
        );

        // Drain: three executions, then the job is terminal and further runs are no-ops.
        for (var i = 0; i < 5; i++)
        {
            await worker.RunOnceAsync();
        }

        Assert.Equal(3, executions);
        var job = await Store.GetAsync(jobId);
        Assert.Equal(JobStatus.Failed, job!.Status);
        Assert.Equal(3, job.Attempt);
        Assert.Contains("boom", job.Error);
    }

    [Fact]
    public async Task Long_running_activity_is_not_re_executed_while_its_worker_is_alive()
    {
        // The fix under test: a healthy worker heartbeats its lease, so a slow activity
        // (longer than LeaseDuration) is never stolen and re-run by another worker.
        var executions = 0;
        var registry = new ActivityRegistry();
        registry.Register<object?, string>(
            "slow",
            async (_, _, ct) =>
            {
                Interlocked.Increment(ref executions);
                await Task.Delay(TimeSpan.FromSeconds(2.5), ct);
                return "done";
            }
        );
        var jobId = await Store.EnqueueAsync("slow", (object?)null, maxAttempts: 5);

        var lease = TimeSpan.FromSeconds(1);
        var workerA = new ActivityWorker(registry, Store, Options("A") with { LeaseDuration = lease });
        var workerB = new ActivityWorker(registry, Store, Options("B") with { LeaseDuration = lease });

        var runA = workerA.RunOnceAsync().AsTask();
        // Past the 1s lease: without heartbeat, B would reclaim and double-execute here.
        await Task.Delay(TimeSpan.FromMilliseconds(1300));
        var runB = workerB.RunOnceAsync().AsTask();

        await Task.WhenAll(runA, runB);

        Assert.Equal(1, executions);
        Assert.Equal(0, await runB); // B found nothing to lease
        var job = await Store.GetAsync(jobId);
        Assert.Equal(JobStatus.Succeeded, job!.Status);
        Assert.Equal("done", job.GetResult<string>());
        Assert.Equal(1, job.Attempt); // attempt not inflated by a re-lease
    }

    [Fact]
    public async Task Job_is_reclaimed_and_completed_by_another_worker_when_its_worker_dies()
    {
        // The complement of the heartbeat test: when a worker stops renewing (here we
        // cancel it mid-flight to simulate a crash), the lease expires and another
        // worker reclaims and completes the job.
        var started = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var registry = new ActivityRegistry();
        registry.Register<object?, string>(
            "stalls-then-recovers",
            async (ctx, _, ct) =>
            {
                if (ctx.Attempt == 1)
                {
                    started.TrySetResult();
                    await Task.Delay(Timeout.InfiniteTimeSpan, ct); // hang until "crash"
                }

                return "recovered";
            }
        );
        var jobId = await Store.EnqueueAsync("stalls-then-recovers", (object?)null, maxAttempts: 5);

        var lease = TimeSpan.FromSeconds(1);
        var deadWorker = new ActivityWorker(registry, Store, Options("dead") with { LeaseDuration = lease });
        var liveWorker = new ActivityWorker(registry, Store, Options("live") with { LeaseDuration = lease });

        using var crash = new CancellationTokenSource();
        var deadRun = deadWorker.RunAsync(crash.Token);
        await started.Task; // first attempt is leased and executing

        crash.Cancel(); // worker process "dies" — stops renewing its lease
        await SwallowCancellation(deadRun);

        // The live worker should reclaim after the lease expires and finish the job.
        var job = await RunUntilTerminalAsync(liveWorker, jobId, TimeSpan.FromSeconds(10));

        Assert.Equal(JobStatus.Succeeded, job.Status);
        Assert.Equal("recovered", job.GetResult<string>());
        Assert.True(job.Attempt >= 2, $"expected a re-lease, got attempt {job.Attempt}");
    }

    [Fact]
    public async Task Stale_lease_holders_commit_is_rejected_so_a_job_completes_once()
    {
        // The write-time half of exactly-once completion: if a worker's lease was
        // reclaimed while it worked, its commit must hit zero rows and be rejected,
        // leaving the current holder's result as the one that stands. (The crash/reclaim
        // E2E path abandons before committing, so this guard is only reachable directly.)
        var past = DateTimeOffset.UtcNow.AddMinutes(-2);
        var jobId = await Store.EnqueueAsync(
            new EnqueueActivityRequest("x", null, MaxAttempts: 5, VisibleAt: past)
        );

        // First holder leases as-of the past with a short duration, so its lease is
        // already expired by the time the second worker reclaims as-of now.
        var stale = await Store.LeaseAsync(
            new LeaseActivityJobsRequest("old", 1, TimeSpan.FromSeconds(1), past.AddSeconds(30))
        );
        var fresh = await Store.LeaseAsync(
            new LeaseActivityJobsRequest("new", 1, TimeSpan.FromSeconds(30), DateTimeOffset.UtcNow)
        );

        Assert.Equal(jobId, Assert.Single(stale).JobId);
        Assert.Equal(jobId, Assert.Single(fresh).JobId);
        Assert.NotEqual(stale[0].LeaseToken, fresh[0].LeaseToken);

        Assert.False(await Store.RecordSuccessAsync(jobId, stale[0].LeaseToken, "\"stale\""));
        Assert.True(await Store.RecordSuccessAsync(jobId, fresh[0].LeaseToken, "\"fresh\""));

        var job = await Store.GetAsync(jobId);
        Assert.Equal(JobStatus.Succeeded, job!.Status);
        Assert.Equal("fresh", job.GetResult<string>());
    }

    [Fact]
    public async Task Jobs_leased_beyond_the_concurrency_limit_are_not_lost_to_lease_expiry()
    {
        // Regression: a worker must not lease more jobs than it can keep alive. With
        // BatchSize > MaxConcurrency, the jobs queued behind the concurrency limit have
        // no renewer running while they wait, so their lease lapses and a second worker
        // reclaims and re-executes them — exactly the back-of-batch double-execution the
        // heartbeat work set out to prevent.
        var firstLeased = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var executions = new ConcurrentDictionary<Guid, int>();
        var registry = new ActivityRegistry();
        registry.Register<string, string>(
            "slow",
            async (ctx, _, ct) =>
            {
                executions.AddOrUpdate(ctx.JobId, 1, static (_, n) => n + 1);
                firstLeased.TrySetResult();
                await Task.Delay(TimeSpan.FromMilliseconds(600), ct);
                return "done";
            }
        );

        var ids = new List<Guid>();
        for (var i = 0; i < 3; i++)
        {
            ids.Add(await Store.EnqueueAsync("slow", i.ToString(), maxAttempts: 5));
        }

        var lease = TimeSpan.FromMilliseconds(400);
        // A grabs the batch but runs one at a time; B runs anything reclaimable. Starting B
        // only once A's first handler is running guarantees A won the batch — without that,
        // B might lease everything first and the test wouldn't exercise the bug.
        var workerA = new ActivityWorker(
            registry,
            Store,
            Options("A") with { BatchSize = 3, MaxConcurrency = 1, LeaseDuration = lease }
        );
        var workerB = new ActivityWorker(
            registry,
            Store,
            Options("B") with { BatchSize = 3, MaxConcurrency = 3, LeaseDuration = lease }
        );

        using var cts = new CancellationTokenSource();
        var runA = workerA.RunAsync(cts.Token);
        await firstLeased.Task; // A has leased its batch and begun the first job
        var runB = workerB.RunAsync(cts.Token);

        // Long enough for A to serially work through all three jobs it leased.
        await Task.Delay(TimeSpan.FromSeconds(2.5));
        cts.Cancel();
        await SwallowCancellation(runA);
        await SwallowCancellation(runB);

        var jobs = await Task.WhenAll(ids.Select(id => Store.GetAsync(id).AsTask()));
        Assert.All(jobs, j => Assert.Equal(JobStatus.Succeeded, j!.Status));
        Assert.All(ids, id => Assert.Equal(1, executions.GetValueOrDefault(id)));
    }

    [Fact]
    public async Task A_failed_commit_does_not_cancel_sibling_jobs_or_kill_the_worker()
    {
        // Regression: when one job's outcome fails to record (a real store error), it must
        // not fault the Parallel batch — the sibling jobs running alongside it must still
        // finish — nor kill the worker loop. The failed job stays leased and is retried
        // after its lease expires.
        const int jobCount = 4;
        var executions = new ConcurrentDictionary<Guid, int>();
        var registry = new ActivityRegistry();
        registry.Register<string, string>(
            "ok",
            async (ctx, _, ct) =>
            {
                executions.AddOrUpdate(ctx.JobId, 1, static (_, n) => n + 1);
                await Task.Delay(50, ct);
                return "ok";
            }
        );

        var ids = new List<Guid>();
        for (var i = 0; i < jobCount; i++)
        {
            ids.Add(await Store.EnqueueAsync("ok", i.ToString(), maxAttempts: 5));
        }

        // Fails exactly the first commit; every other write records normally.
        var flaky = new FailFirstSuccessStore(Store);
        var worker = new ActivityWorker(
            registry,
            flaky,
            Options("w") with
            {
                BatchSize = jobCount,
                MaxConcurrency = jobCount,
                LeaseDuration = TimeSpan.FromMilliseconds(400),
            }
        );

        using var cts = new CancellationTokenSource();
        var run = worker.RunAsync(cts.Token);

        foreach (var id in ids)
        {
            await WaitForTerminalAsync(id, TimeSpan.FromSeconds(10));
        }

        cts.Cancel();
        await SwallowCancellation(run);

        var jobs = await Task.WhenAll(ids.Select(id => Store.GetAsync(id).AsTask()));
        Assert.All(jobs, j => Assert.Equal(JobStatus.Succeeded, j!.Status));
        Assert.True(flaky.SuccessAttempts > jobCount, "the failed commit should have been retried");
        // Exactly one job's commit failed and was retried (one extra execution). If the
        // failure had faulted the batch, the three siblings would have been cancelled
        // mid-run and re-executed too, pushing the total above jobCount + 1.
        Assert.Equal(jobCount + 1, executions.Values.Sum());
    }

    private async Task<ActivityJob> WaitForTerminalAsync(Guid jobId, TimeSpan timeout)
    {
        using var deadline = new CancellationTokenSource(timeout);
        while (!deadline.IsCancellationRequested)
        {
            var job = await Store.GetAsync(jobId);
            if (job is { Status: JobStatus.Succeeded or JobStatus.Failed })
            {
                return job;
            }

            await Task.Delay(100, deadline.Token);
        }

        throw new TimeoutException($"Job {jobId} did not reach a terminal state in {timeout}.");
    }

    private async Task<long> CountJobsAsync(string activityName, string? idempotencyKey = null)
    {
        var sql = idempotencyKey is null
            ? "select count(*) from pw_activity_jobs where activity_name = @activity_name and idempotency_key is null;"
            : "select count(*) from pw_activity_jobs where activity_name = @activity_name and idempotency_key = @idempotency_key;";

        await using var command = DataSource.CreateCommand(sql);
        command.Parameters.AddWithValue("activity_name", activityName);
        if (idempotencyKey is not null)
        {
            command.Parameters.AddWithValue("idempotency_key", idempotencyKey);
        }

        return (long)(await command.ExecuteScalarAsync())!;
    }

    /// <summary>Wraps the real store and throws on the first RecordSuccess to simulate a
    /// transient store error at commit time.</summary>
    private sealed class FailFirstSuccessStore(IActivityJobStore inner) : IActivityJobStore
    {
        private int _successAttempts;

        public int SuccessAttempts => Volatile.Read(ref _successAttempts);

        public ValueTask<Guid> EnqueueAsync(EnqueueActivityRequest r, CancellationToken ct = default) =>
            inner.EnqueueAsync(r, ct);

        public ValueTask<IReadOnlyList<LeasedActivityJob>> LeaseAsync(LeaseActivityJobsRequest r, CancellationToken ct = default) =>
            inner.LeaseAsync(r, ct);

        public ValueTask<ActivityJob?> GetAsync(Guid id, CancellationToken ct = default) =>
            inner.GetAsync(id, ct);

        public ValueTask<bool> RenewLeaseAsync(Guid id, string token, DateTimeOffset exp, CancellationToken ct = default) =>
            inner.RenewLeaseAsync(id, token, exp, ct);

        public ValueTask<bool> RecordSuccessAsync(Guid id, string token, string? resultJson, CancellationToken ct = default)
        {
            if (Interlocked.Increment(ref _successAttempts) == 1)
            {
                throw new InvalidOperationException("simulated transient store error");
            }

            return inner.RecordSuccessAsync(id, token, resultJson, ct);
        }

        public ValueTask<bool> RecordFailureAsync(Guid id, string token, string error, bool retryable, DateTimeOffset? next, CancellationToken ct = default) =>
            inner.RecordFailureAsync(id, token, error, retryable, next, ct);
    }

    private static ActivityWorkerOptions Options(string workerId) =>
        new()
        {
            WorkerId = workerId,
            BatchSize = 1,
            PollInterval = TimeSpan.FromMilliseconds(100),
        };

    private async Task<ActivityJob> RunUntilTerminalAsync(
        ActivityWorker worker,
        Guid jobId,
        TimeSpan timeout
    )
    {
        using var deadline = new CancellationTokenSource(timeout);
        while (!deadline.IsCancellationRequested)
        {
            await worker.RunOnceAsync(deadline.Token);
            var job = await Store.GetAsync(jobId);
            if (job is { Status: JobStatus.Succeeded or JobStatus.Failed })
            {
                return job;
            }

            await Task.Delay(100, deadline.Token);
        }

        throw new TimeoutException($"Job {jobId} did not reach a terminal state in {timeout}.");
    }

    private sealed record GreetingInput(string Name);

    private sealed record GreetingOutput(string Message);

    private sealed class GreetingActivities
    {
        [Activity("uppercase")]
        public GreetingOutput Uppercase(GreetingInput input) =>
            new(input.Name.ToUpperInvariant());
    }

    private sealed record GreetingPrefix(string Value);

    private sealed class HostedGreetingActivities(GreetingPrefix prefix)
    {
        [Activity("hosted-greet")]
        public string Greet(string input) => $"{prefix.Value} {input}";

        [Activity("hosted-repeat")]
        public ValueTask<string> Repeat(int count, CancellationToken _) =>
            ValueTask.FromResult(string.Join(' ', Enumerable.Repeat(prefix.Value, count)));
    }
}
