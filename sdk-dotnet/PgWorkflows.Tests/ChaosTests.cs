using PgWorkflows.Activities;
using PgWorkflows.Jobs;
using PgWorkflows.Workers;
using Xunit;

namespace PgWorkflows.Tests;

/// <summary>
/// The integration of the mechanisms the deterministic tests isolate (SKIP LOCKED,
/// lease reclaim, heartbeat), exercised under sustained worker crash/restart churn.
/// Its single job: prove the queue still <b>drains every job</b> — nothing lost, nothing
/// stuck in <c>leased</c> — when workers keep dying mid-flight. Exactly-once completion
/// of an individual job is covered deterministically by <see cref="WorkerTests"/>.
/// </summary>
public sealed class ChaosTests(PostgresFixture fixture) : PostgresTestBase(fixture)
{
    [Fact]
    public async Task Workers_crashing_and_restarting_still_drain_every_job()
    {
        const int jobCount = 60;

        var registry = new ActivityRegistry();
        registry.Register(
            "work",
            async (_, _, ct) =>
            {
                // Each job runs a little longer than the 400ms lease, so survivors must
                // heartbeat to keep it and a crash reliably lands while a job is in flight.
                await Task.Delay(TimeSpan.FromMilliseconds(500), ct);
                return "ok";
            }
        );

        for (var i = 0; i < jobCount; i++)
        {
            await Store.EnqueueAsync(new EnqueueActivityRequest("work", null, MaxAttempts: 100));
        }

        var options = new ActivityWorkerOptions
        {
            BatchSize = 4,
            MaxConcurrency = 4,
            LeaseDuration = TimeSpan.FromMilliseconds(400),
            PollInterval = TimeSpan.FromMilliseconds(50),
        };

        var running = new List<Task>();
        var workers = new CancellationTokenSource[4];
        for (var i = 0; i < workers.Length; i++)
        {
            workers[i] = new CancellationTokenSource();
            running.Add(new ActivityWorker(registry, Store, options).RunAsync(workers[i].Token));
        }

        // Crash a worker every 250ms and immediately replace it, so its in-flight jobs are
        // abandoned mid-lease and must be reclaimed by a survivor after the lease expires.
        for (var crash = 0; crash < 12; crash++)
        {
            await Task.Delay(250);
            var slot = crash % workers.Length;
            workers[slot].Cancel();
            workers[slot] = new CancellationTokenSource();
            running.Add(new ActivityWorker(registry, Store, options).RunAsync(workers[slot].Token));
        }

        // Stop churning, let the survivors finish, then shut everyone down.
        await PollUntilSucceededAsync(jobCount, TimeSpan.FromSeconds(30));

        foreach (var cts in workers)
        {
            cts.Cancel();
        }

        await Task.WhenAll(running.Select(SwallowCancellation));
    }

    private async Task PollUntilSucceededAsync(int target, TimeSpan timeout)
    {
        using var deadline = new CancellationTokenSource(timeout);
        while (true)
        {
            await using var command = DataSource.CreateCommand(
                "select count(*) from pw_activity_jobs where status = 'succeeded';"
            );
            var succeeded = (long)(await command.ExecuteScalarAsync())!;
            if (succeeded >= target)
            {
                return;
            }

            if (deadline.IsCancellationRequested)
            {
                throw new TimeoutException(
                    $"Only {succeeded}/{target} jobs succeeded before the churn test timed out."
                );
            }

            await Task.Delay(100);
        }
    }
}
