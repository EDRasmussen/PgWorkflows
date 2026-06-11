using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using Npgsql;
using PgWorkflows.Activities;
using PgWorkflows.Workers;
using PgWorkflows.Workflows;
using Xunit;

namespace PgWorkflows.Tests;

public sealed class WorkflowTests(PostgresFixture fixture) : PostgresTestBase(fixture)
{
    [Fact]
    public async Task Client_executes_registered_workflow()
    {
        var activities = new TestActivities();
        var registry = new ActivityRegistry();
        registry.Register("wf-greet", (string input) => activities.Greet(input));
        registry.Register("wf-upper", (string input) => activities.Uppercase(input));
        var worker = new ActivityWorker(registry, Store, Options("activity-worker"));
        var client = CreateClient<ClientGreetingWorkflow>();

        using var workerCts = new CancellationTokenSource();
        var workerRun = worker.RunAsync(workerCts.Token);

        try
        {
            var result = await client.ExecuteAsync<ClientGreetingWorkflow, string, string>("world");

            Assert.Equal("HELLO WORLD", result);
            Assert.Equal(1, activities.GreetExecutions);
            Assert.Equal(1, activities.UppercaseExecutions);

            var run = await SingleWorkflowRunAsync();
            var firstStep = await WorkflowStore.GetStepAsync(run.WorkflowRunId, 0);
            var secondStep = await WorkflowStore.GetStepAsync(run.WorkflowRunId, 1);
            Assert.Equal(WorkflowStatus.Succeeded, run.Status);
            Assert.Equal(WorkflowStepStatus.Succeeded, firstStep!.Status);
            Assert.Equal(WorkflowStepStatus.Succeeded, secondStep!.Status);
            Assert.Equal("HELLO WORLD", System.Text.Json.JsonSerializer.Deserialize<string>(run.ResultJson!));
        }
        finally
        {
            workerCts.Cancel();
            await SwallowCancellation(workerRun);
        }
    }

    [Fact]
    public async Task Client_start_with_same_idempotency_key_returns_existing_workflow_run()
    {
        var client = CreateClient<ClientGreetingWorkflow>();

        var first = await client.StartAsync<ClientGreetingWorkflow, string, string>(
            "first",
            idempotencyKey: "workflow-key-1"
        );
        var second = await client.StartAsync<ClientGreetingWorkflow, string, string>(
            "second",
            idempotencyKey: "workflow-key-1"
        );

        Assert.Equal(first.WorkflowRunId, second.WorkflowRunId);
        var run = await WorkflowStore.GetRunAsync(first.WorkflowRunId);
        Assert.Equal("workflow-key-1", run!.IdempotencyKey);
    }

    [Fact]
    public void Workflow_registry_rejects_duplicate_workflow_names()
    {
        var registry = new WorkflowRegistry();
        registry.Register<DuplicateNameWorkflowA>();

        var ex = Assert.Throws<InvalidOperationException>(
            () => registry.Register<DuplicateNameWorkflowB>()
        );

        Assert.Contains("duplicate-workflow-name", ex.Message);
    }

    [Fact]
    public async Task Workflow_activity_call_uses_activity_attribute_name()
    {
        var activities = new CustomNamedActivities();
        var registry = new ActivityRegistry();
        registry.Register("custom-greet", (string name) => activities.Greet(name));
        var worker = new ActivityWorker(registry, Store, Options("activity-worker"));
        var client = CreateClient<CustomNamedWorkflow>();

        using var workerCts = new CancellationTokenSource();
        var workerRun = worker.RunAsync(workerCts.Token);

        try
        {
            var result = await client.ExecuteAsync<CustomNamedWorkflow, string, string>("world");

            Assert.Equal("custom world", result);
            Assert.Equal(1, activities.Executions);
        }
        finally
        {
            workerCts.Cancel();
            await SwallowCancellation(workerRun);
        }
    }

    [Fact]
    public async Task Workflow_resume_returns_completed_step_without_reexecuting_activity()
    {
        var activities = new TestActivities();
        var registry = new ActivityRegistry();
        registry.Register("wf-greet", (string input) => activities.Greet(input));
        registry.Register("wf-upper", (string input) => activities.Uppercase(input));
        var worker = new ActivityWorker(registry, Store, Options("activity-worker"));
        var client = CreateClient<ResumableGreetingWorkflow>();
        var handle = await client.StartAsync<ResumableGreetingWorkflow, string, string>("world");

        using var workerCts = new CancellationTokenSource();
        var workerRun = worker.RunAsync(workerCts.Token);

        try
        {
            using var firstAttemptCts = new CancellationTokenSource();
            ResumableGreetingWorkflow.Reset(firstAttemptCts);
            await Assert.ThrowsAsync<OperationCanceledException>(async () =>
                await handle.GetResultAsync(firstAttemptCts.Token)
            );

            Assert.Equal(1, activities.GreetExecutions);
            Assert.Equal(0, activities.UppercaseExecutions);

            ResumableGreetingWorkflow.Reset(cancelAfterFirstStep: null);
            var result = await handle.GetResultAsync();

            Assert.Equal("HELLO WORLD", result);
            Assert.Equal(1, activities.GreetExecutions);
            Assert.Equal(1, activities.UppercaseExecutions);

            var firstStep = await WorkflowStore.GetStepAsync(handle.WorkflowRunId, 0);
            var secondStep = await WorkflowStore.GetStepAsync(handle.WorkflowRunId, 1);
            Assert.Equal(WorkflowStepStatus.Succeeded, firstStep!.Status);
            Assert.Equal(WorkflowStepStatus.Succeeded, secondStep!.Status);

            var run = await WorkflowStore.GetRunAsync(handle.WorkflowRunId);
            Assert.Equal(WorkflowStatus.Succeeded, run!.Status);
        }
        finally
        {
            workerCts.Cancel();
            await SwallowCancellation(workerRun);
        }
    }

    [Fact]
    public async Task Workflow_when_all_resumes_completed_fanout_without_reexecuting_activities()
    {
        var activities = new FanOutActivities();
        var registry = new ActivityRegistry();
        registry.Register<string, string>("wf-fanout-echo", activities.EchoAsync);
        registry.Register("wf-fanout-join", (string input) => activities.Join(input));
        var worker = new ActivityWorker(
            registry,
            Store,
            Options("activity-worker") with
            {
                BatchSize = 2,
                MaxConcurrency = 2,
            }
        );
        var client = CreateClient<ResumableFanOutWorkflow>();
        var handle = await client.StartAsync<ResumableFanOutWorkflow, FanOutInput, string>(
            new FanOutInput("hello", "world")
        );

        using var workerCts = new CancellationTokenSource();
        var workerRun = worker.RunAsync(workerCts.Token);

        try
        {
            using var firstAttemptCts = new CancellationTokenSource();
            ResumableFanOutWorkflow.Reset(firstAttemptCts);
            await Assert.ThrowsAsync<OperationCanceledException>(async () =>
                await handle.GetResultAsync(firstAttemptCts.Token)
            );

            Assert.Equal(2, activities.EchoExecutions);
            Assert.Equal(0, activities.JoinExecutions);
            Assert.Equal(2, activities.MaxConcurrentEchoes);

            ResumableFanOutWorkflow.Reset(cancelAfterFanOut: null);
            var result = await handle.GetResultAsync();

            Assert.Equal("hello world", result);
            Assert.Equal(2, activities.EchoExecutions);
            Assert.Equal(1, activities.JoinExecutions);

            var leftStep = await WorkflowStore.GetStepAsync(handle.WorkflowRunId, 0);
            var rightStep = await WorkflowStore.GetStepAsync(handle.WorkflowRunId, 1);
            var joinStep = await WorkflowStore.GetStepAsync(handle.WorkflowRunId, 2);
            Assert.Equal(WorkflowStepStatus.Succeeded, leftStep!.Status);
            Assert.Equal(WorkflowStepStatus.Succeeded, rightStep!.Status);
            Assert.Equal(WorkflowStepStatus.Succeeded, joinStep!.Status);
        }
        finally
        {
            workerCts.Cancel();
            await SwallowCancellation(workerRun);
        }
    }

    [Fact]
    public async Task Workflow_when_all_records_mixed_success_and_failure_steps()
    {
        var activities = new MixedFanOutActivities();
        var registry = new ActivityRegistry();
        registry.Register("wf-mixed-ok", (string input) => activities.Ok(input));
        registry.Register("wf-mixed-fail", (string input) => activities.Fail(input));
        var worker = new ActivityWorker(
            registry,
            Store,
            Options("activity-worker") with
            {
                BatchSize = 2,
                MaxConcurrency = 2,
            }
        );
        var client = CreateClient<MixedFailureFanOutWorkflow>();

        using var workerCts = new CancellationTokenSource();
        var workerRun = worker.RunAsync(workerCts.Token);

        try
        {
            var ex = await Assert.ThrowsAsync<InvalidOperationException>(async () =>
                await client.ExecuteAsync<MixedFailureFanOutWorkflow, string, string>("value")
            );

            Assert.Contains("Workflow activity step 1 failed", ex.Message);
            Assert.Equal(1, activities.OkExecutions);
            Assert.Equal(1, activities.FailExecutions);

            var run = await SingleWorkflowRunAsync();
            var okStep = await WorkflowStore.GetStepAsync(run.WorkflowRunId, 0);
            var failedStep = await WorkflowStore.GetStepAsync(run.WorkflowRunId, 1);
            Assert.Equal(WorkflowStatus.Failed, run.Status);
            Assert.Equal(WorkflowStepStatus.Succeeded, okStep!.Status);
            Assert.Equal(WorkflowStepStatus.Failed, failedStep!.Status);
            Assert.Contains("boom value", failedStep.Error);
        }
        finally
        {
            workerCts.Cancel();
            await SwallowCancellation(workerRun);
        }
    }

    [Fact]
    public async Task Hosted_workflow_worker_processes_started_workflow_without_caller_driving_result()
    {
        var activities = new TestActivities();
        var services = new ServiceCollection();
        services.AddSingleton(activities);
        services.AddPgWorkflows(pg =>
            pg.UsePostgres(DataSource, ensureSchemaOnStart: false)
                .ConfigureActivityWorker(options =>
                    options with
                    {
                        WorkerId = "hosted-activity-worker",
                        BatchSize = 1,
                        PollInterval = TimeSpan.FromMilliseconds(10),
                    }
                )
                .ConfigureWorkflowWorker(options =>
                    options with
                    {
                        WorkerId = "hosted-workflow-worker",
                        BatchSize = 1,
                        PollInterval = TimeSpan.FromMilliseconds(10),
                        // Short backstop so a rare missed wake recovers within the test timeout.
                        ParkGrace = TimeSpan.FromSeconds(2),
                    }
                )
                .AddActivities<TestActivities>()
                .AddWorkflow<ClientGreetingWorkflow>()
        );

        await using var provider = services.BuildServiceProvider();
        var hostedService = Assert.Single(provider.GetServices<IHostedService>());
        await hostedService.StartAsync(CancellationToken.None);

        try
        {
            var client = provider.GetRequiredService<IPgWorkflowClient>();
            var handle = await client.StartAsync<ClientGreetingWorkflow, string, string>("world");
            var run = await WaitForWorkflowTerminalAsync(handle.WorkflowRunId, TimeSpan.FromSeconds(10));
            var result = await handle.GetResultAsync();

            Assert.Equal(WorkflowStatus.Succeeded, run.Status);
            Assert.Equal("HELLO WORLD", result);
            Assert.Equal(1, activities.GreetExecutions);
            Assert.Equal(1, activities.UppercaseExecutions);
        }
        finally
        {
            await hostedService.StopAsync(CancellationToken.None);
        }
    }

    [Fact]
    public async Task Client_only_process_dispatches_workflow_to_separate_worker_process()
    {
        var activities = new TestActivities();

        var clientServices = new ServiceCollection();
        clientServices.AddPgWorkflows(pg =>
            pg.UsePostgres(DataSource, ensureSchemaOnStart: false)
                .DisableWorkers()
                .AddWorkflow<ClientGreetingWorkflow>()
        );

        var workerServices = new ServiceCollection();
        workerServices.AddSingleton(activities);
        workerServices.AddPgWorkflows(pg =>
            pg.UsePostgres(DataSource, ensureSchemaOnStart: false)
                .ConfigureActivityWorker(options =>
                    options with
                    {
                        WorkerId = "fleet-activity-worker",
                        BatchSize = 1,
                        PollInterval = TimeSpan.FromMilliseconds(10),
                    }
                )
                .ConfigureWorkflowWorker(options =>
                    options with
                    {
                        WorkerId = "fleet-workflow-worker",
                        BatchSize = 1,
                        PollInterval = TimeSpan.FromMilliseconds(10),
                        // Short backstop so a rare missed wake recovers within the test timeout.
                        ParkGrace = TimeSpan.FromSeconds(2),
                    }
                )
                .AddActivities<TestActivities>()
                .AddWorkflow<ClientGreetingWorkflow>()
        );

        await using var clientProvider = clientServices.BuildServiceProvider();
        await using var workerProvider = workerServices.BuildServiceProvider();

        var clientHosted = Assert.Single(clientProvider.GetServices<IHostedService>());
        var workerHosted = Assert.Single(workerProvider.GetServices<IHostedService>());
        await clientHosted.StartAsync(CancellationToken.None);

        try
        {
            var client = clientProvider.GetRequiredService<IPgWorkflowClient>();
            var handle = await client.StartAsync<ClientGreetingWorkflow, string, string>("world");

            // Only the client-only host is running, so nothing can process the run.
            await Task.Delay(200);
            var pendingRun = await WorkflowStore.GetRunAsync(
                handle.WorkflowRunId,
                CancellationToken.None
            );
            Assert.NotNull(pendingRun);
            Assert.Equal(WorkflowStatus.Pending, pendingRun.Status);
            Assert.Equal(0, activities.GreetExecutions);

            await workerHosted.StartAsync(CancellationToken.None);

            var run = await WaitForWorkflowTerminalAsync(
                handle.WorkflowRunId,
                TimeSpan.FromSeconds(10)
            );
            var result = await handle.GetResultAsync();

            Assert.Equal(WorkflowStatus.Succeeded, run.Status);
            Assert.Equal("HELLO WORLD", result);
            Assert.Equal(1, activities.GreetExecutions);
            Assert.Equal(1, activities.UppercaseExecutions);
        }
        finally
        {
            await workerHosted.StopAsync(CancellationToken.None);
            await clientHosted.StopAsync(CancellationToken.None);
        }
    }

    [Fact]
    public async Task Many_activity_waiting_workflows_wake_promptly_without_waiting_out_park_grace()
    {
        // Schedule->park lost-wake race: with many workflows each scheduling an activity that
        // completes fast, a completion can land while its parent is still mid-park. A lost wake
        // strands that run until ParkGrace, set here far above the assertion window so any lost
        // wake trips the deadline instead of hiding behind a short backstop.
        const int workflowCount = 50;
        var services = new ServiceCollection();
        services.AddSingleton(new TestActivities());
        services.AddPgWorkflows(pg =>
            pg.UsePostgres(DataSource, ensureSchemaOnStart: false)
                .ConfigureActivityWorker(options =>
                    options with
                    {
                        WorkerId = "fleet-activity-worker",
                        MaxConcurrency = 16,
                        PollInterval = TimeSpan.FromMilliseconds(10),
                    }
                )
                .ConfigureWorkflowWorker(options =>
                    options with
                    {
                        WorkerId = "fleet-workflow-worker",
                        MaxConcurrency = 16,
                        PollInterval = TimeSpan.FromMilliseconds(10),
                        ParkGrace = TimeSpan.FromSeconds(30),
                    }
                )
                .AddActivities<TestActivities>()
                .AddWorkflow<ClientGreetingWorkflow>()
        );

        await using var provider = services.BuildServiceProvider();
        var hostedService = Assert.Single(provider.GetServices<IHostedService>());
        await hostedService.StartAsync(CancellationToken.None);

        try
        {
            var client = provider.GetRequiredService<IPgWorkflowClient>();
            var runIds = new List<Guid>();
            for (var i = 0; i < workflowCount; i++)
            {
                var handle = await client.StartAsync<ClientGreetingWorkflow, string, string>(
                    $"user-{i}"
                );
                runIds.Add(handle.WorkflowRunId);
            }

            foreach (var runId in runIds)
            {
                var run = await WaitForWorkflowTerminalAsync(runId, TimeSpan.FromSeconds(15));
                Assert.Equal(WorkflowStatus.Succeeded, run.Status);
            }
        }
        finally
        {
            await hostedService.StopAsync(CancellationToken.None);
        }
    }

    [Fact]
    public async Task Hosted_workflow_worker_treats_recorded_workflow_failure_as_processed()
    {
        var workflowRegistry = new WorkflowRegistry();
        workflowRegistry.Register<FailingWorkflow>();
        using var provider = new ServiceCollection().BuildServiceProvider();
        var runner = new WorkflowRunner(WorkflowStore, Store)
        {
            ActivityPollInterval = TimeSpan.FromMilliseconds(10),
            // Short backstop so any rare missed edge-trigger wake recovers well within test timeouts;
            // production keeps the longer default. The edge-trigger handles the common case.
            ParkGrace = TimeSpan.FromSeconds(2),
        };
        var client = new PgWorkflowClient(workflowRegistry, runner, provider);
        var worker = new WorkflowWorker(
            workflowRegistry,
            WorkflowStore,
            runner,
            provider,
            WorkflowWorkerOptions()
        );
        var handle = await client.StartAsync<FailingWorkflow, string, string>("world");

        var processed = await worker.RunOnceAsync();

        Assert.Equal(1, processed);
        var run = await WorkflowStore.GetRunAsync(handle.WorkflowRunId);
        Assert.Equal(WorkflowStatus.Failed, run!.Status);
        Assert.Contains("boom world", run.Error);
        Assert.Equal(0, await worker.RunOnceAsync());
    }

    [Fact]
    public async Task Workflow_worker_releases_run_after_transient_infrastructure_error_without_consuming_an_attempt()
    {
        var workflowRegistry = new WorkflowRegistry();
        workflowRegistry.Register<TransientInfrastructureFailureWorkflow>();
        using var provider = new ServiceCollection().BuildServiceProvider();
        var runner = new WorkflowRunner(WorkflowStore, Store)
        {
            ActivityPollInterval = TimeSpan.FromMilliseconds(10),
        };
        var client = new PgWorkflowClient(workflowRegistry, runner, provider);
        var worker = new WorkflowWorker(
            workflowRegistry,
            WorkflowStore,
            runner,
            provider,
            WorkflowWorkerOptions() with { GetRetryDelay = _ => TimeSpan.Zero }
        );
        TransientInfrastructureFailureWorkflow.Reset();
        var handle = await client.StartAsync<TransientInfrastructureFailureWorkflow, string, string>(
            "world"
        );

        // The transient error is rethrown so the worker loop backs off, but the run itself is
        // released back to pending rather than failed.
        await Assert.ThrowsAsync<AggregateException>(async () => await worker.RunOnceAsync());

        var released = await WorkflowStore.GetRunAsync(handle.WorkflowRunId);
        Assert.Equal(WorkflowStatus.Pending, released!.Status);
        Assert.Equal(0, released.Attempt);
        Assert.Null(released.Error);

        // MaxAttempts is 1; this retry only exists because the release rolled the attempt back.
        Assert.Equal(1, await worker.RunOnceAsync());
        Assert.Equal("hello world", await handle.GetResultAsync());
    }

    [Fact]
    public async Task Hosted_workflow_worker_retries_failed_workflow_without_reexecuting_completed_steps()
    {
        var activities = new RetryActivities();
        var registry = new ActivityRegistry();
        registry.Register("wf-retry-echo", (string input) => activities.Echo(input));
        var activityWorker = new ActivityWorker(registry, Store, Options("activity-worker"));
        var workflowRegistry = new WorkflowRegistry();
        workflowRegistry.Register<RetryAfterActivityWorkflow>();
        using var provider = new ServiceCollection().BuildServiceProvider();
        var runner = new WorkflowRunner(WorkflowStore, Store)
        {
            ActivityPollInterval = TimeSpan.FromMilliseconds(10),
            // Short backstop so any rare missed edge-trigger wake recovers well within test timeouts;
            // production keeps the longer default. The edge-trigger handles the common case.
            ParkGrace = TimeSpan.FromSeconds(2),
        };
        var client = new PgWorkflowClient(workflowRegistry, runner, provider);
        var workflowWorker = new WorkflowWorker(
            workflowRegistry,
            WorkflowStore,
            runner,
            provider,
            WorkflowWorkerOptions() with
            {
                MaxAttempts = 2,
                GetRetryDelay = _ => TimeSpan.Zero,
            }
        );
        RetryAfterActivityWorkflow.Reset();
        var handle = await client.StartAsync<RetryAfterActivityWorkflow, string, string>("value");

        using var activityWorkerCts = new CancellationTokenSource();
        var activityWorkerRun = activityWorker.RunAsync(activityWorkerCts.Token);
        using var workflowWorkerCts = new CancellationTokenSource();
        var workflowWorkerRun = workflowWorker.RunAsync(workflowWorkerCts.Token);

        try
        {
            // The activity parks-and-resumes, then the workflow body fails once (transient) and is
            // retried; the second attempt succeeds. Drive the continuous workers to terminal rather
            // than counting worker passes (parking adds resume cycles), then assert the invariants.
            var succeededRun = await WaitForWorkflowTerminalAsync(
                handle.WorkflowRunId,
                TimeSpan.FromSeconds(10)
            );
            var result = await handle.GetResultAsync();

            Assert.Equal("value", result);
            Assert.Equal(WorkflowStatus.Succeeded, succeededRun.Status);
            // A real retry happened (attempt 2 of 2): the transient first failure spent an attempt,
            // while the activity park did not (it gives its attempt back). The activity body ran
            // exactly once across the retry — its completed step was memoized, not re-executed.
            Assert.Equal(2, succeededRun.Attempt);
            Assert.Equal(2, succeededRun.MaxAttempts);
            Assert.Equal(1, activities.EchoExecutions);
        }
        finally
        {
            workflowWorkerCts.Cancel();
            activityWorkerCts.Cancel();
            await SwallowCancellation(workflowWorkerRun);
            await SwallowCancellation(activityWorkerRun);
        }
    }

    [Fact]
    public async Task Workflow_releases_its_lease_while_a_leased_activity_runs_then_resumes()
    {
        // The core promise of park-and-replay: while a leased run waits on an activity, it holds no
        // worker lease. We gate the activity so it stays in-flight, then observe the run parked
        // (Pending, no lease) mid-activity, release it, and confirm it resumes to success exactly once.
        var activities = new GatedActivities();
        var registry = new ActivityRegistry();
        registry.Register<string, string>("wf-gated", activities.RunAsync);
        var activityWorker = new ActivityWorker(registry, Store, Options("activity-worker"));
        var workflowRegistry = new WorkflowRegistry();
        workflowRegistry.Register<GatedActivityWorkflow>();
        using var provider = new ServiceCollection().BuildServiceProvider();
        var runner = new WorkflowRunner(WorkflowStore, Store)
        {
            ActivityPollInterval = TimeSpan.FromMilliseconds(10),
            // Short backstop so any rare missed edge-trigger wake recovers well within test timeouts;
            // production keeps the longer default. The edge-trigger handles the common case.
            ParkGrace = TimeSpan.FromSeconds(2),
        };
        var client = new PgWorkflowClient(workflowRegistry, runner, provider);
        var workflowWorker = new WorkflowWorker(
            workflowRegistry,
            WorkflowStore,
            runner,
            provider,
            WorkflowWorkerOptions()
        );
        var handle = await client.StartAsync<GatedActivityWorkflow, string, string>("value");

        using var activityWorkerCts = new CancellationTokenSource();
        var activityWorkerRun = activityWorker.RunAsync(activityWorkerCts.Token);
        using var workflowWorkerCts = new CancellationTokenSource();
        var workflowWorkerRun = workflowWorker.RunAsync(workflowWorkerCts.Token);

        try
        {
            // Wait until the activity is actually executing (it then blocks on the gate).
            await activities.Started.Task.WaitAsync(TimeSpan.FromSeconds(10));

            // While the activity runs, the run must be parked with no lease held — that is the
            // feature. (The activity is gated, so it cannot complete and race this observation.)
            var parked = await WaitForWorkflowStatusAsync(
                handle.WorkflowRunId,
                WorkflowStatus.Pending,
                TimeSpan.FromSeconds(10)
            );
            Assert.Equal(WorkflowStatus.Pending, parked.Status);
            Assert.False(await RunLeaseHeldAsync(handle.WorkflowRunId));
            // The step is still in-flight (not yet memoized): the run genuinely yielded mid-activity.
            var stepWhileParked = await WorkflowStore.GetStepAsync(handle.WorkflowRunId, 0);
            Assert.Equal(WorkflowStepStatus.Scheduled, stepWhileParked!.Status);

            // Release the activity: completing it wakes the parked run, which resumes to success.
            activities.Release.SetResult();

            // Wait via the store (handle.GetResultAsync would re-execute inline in this caller and
            // re-run the activity); read the durable result.
            var succeeded = await WaitForWorkflowTerminalAsync(
                handle.WorkflowRunId,
                TimeSpan.FromSeconds(10)
            );
            Assert.Equal(WorkflowStatus.Succeeded, succeeded.Status);
            Assert.Equal(
                "gated:value",
                System.Text.Json.JsonSerializer.Deserialize<string>(succeeded.ResultJson!)
            );
            Assert.Equal(1, activities.Executions);
            var step = await WorkflowStore.GetStepAsync(handle.WorkflowRunId, 0);
            Assert.Equal(WorkflowStepStatus.Succeeded, step!.Status);
        }
        finally
        {
            activities.Release.TrySetResult();
            workflowWorkerCts.Cancel();
            activityWorkerCts.Cancel();
            await SwallowCancellation(workflowWorkerRun);
            await SwallowCancellation(activityWorkerRun);
        }
    }

    [Fact]
    public async Task Leased_when_all_releases_its_lease_while_fanned_out_activities_run_then_resumes()
    {
        // The headline of the feature: a fan-out (ctx.WhenAll) under the workflow worker dispatches
        // every sibling to the queue, then releases its lease while they run — holding no worker —
        // and resumes exactly once when the last sibling completes, with each body run a single time.
        var activities = new GatedFanOutActivities();
        var registry = new ActivityRegistry();
        registry.Register<string, string>("wf-gated-left", activities.LeftAsync);
        registry.Register<string, string>("wf-gated-right", activities.RightAsync);
        var activityWorker = new ActivityWorker(
            registry,
            Store,
            Options("activity-worker") with
            {
                BatchSize = 2,
                MaxConcurrency = 2,
            }
        );
        var workflowRegistry = new WorkflowRegistry();
        workflowRegistry.Register<GatedFanOutWorkflow>();
        using var provider = new ServiceCollection().BuildServiceProvider();
        var runner = new WorkflowRunner(WorkflowStore, Store)
        {
            ActivityPollInterval = TimeSpan.FromMilliseconds(10),
            // Short backstop so any rare missed edge-trigger wake recovers well within test timeouts;
            // production keeps the longer default. The edge-trigger handles the common case.
            ParkGrace = TimeSpan.FromSeconds(2),
        };
        var client = new PgWorkflowClient(workflowRegistry, runner, provider);
        var workflowWorker = new WorkflowWorker(
            workflowRegistry,
            WorkflowStore,
            runner,
            provider,
            WorkflowWorkerOptions()
        );
        var handle = await client.StartAsync<GatedFanOutWorkflow, string, string>("value");

        using var activityWorkerCts = new CancellationTokenSource();
        var activityWorkerRun = activityWorker.RunAsync(activityWorkerCts.Token);
        using var workflowWorkerCts = new CancellationTokenSource();
        var workflowWorkerRun = workflowWorker.RunAsync(workflowWorkerCts.Token);

        try
        {
            // Both siblings are dispatched and running concurrently (then gated).
            await activities.LeftStarted.Task.WaitAsync(TimeSpan.FromSeconds(10));
            await activities.RightStarted.Task.WaitAsync(TimeSpan.FromSeconds(10));

            // While the fan-out runs, the run is parked with no lease held — it yielded its worker.
            var parked = await WaitForWorkflowStatusAsync(
                handle.WorkflowRunId,
                WorkflowStatus.Pending,
                TimeSpan.FromSeconds(10)
            );
            Assert.Equal(WorkflowStatus.Pending, parked.Status);
            Assert.False(await RunLeaseHeldAsync(handle.WorkflowRunId));
            // Both steps are still in-flight (not yet memoized).
            var leftWhileParked = await WorkflowStore.GetStepAsync(handle.WorkflowRunId, 0);
            var rightWhileParked = await WorkflowStore.GetStepAsync(handle.WorkflowRunId, 1);
            Assert.Equal(WorkflowStepStatus.Scheduled, leftWhileParked!.Status);
            Assert.Equal(WorkflowStepStatus.Scheduled, rightWhileParked!.Status);

            // Release both: the last completer wakes the parked run, which resumes and aggregates.
            activities.Release.SetResult();

            var succeeded = await WaitForWorkflowTerminalAsync(
                handle.WorkflowRunId,
                TimeSpan.FromSeconds(10)
            );
            Assert.Equal(WorkflowStatus.Succeeded, succeeded.Status);
            Assert.Equal(
                "L:value R:value",
                System.Text.Json.JsonSerializer.Deserialize<string>(succeeded.ResultJson!)
            );
            // Exactly-once across the park/resume, and both steps durably memoized.
            Assert.Equal(1, activities.LeftExecutions);
            Assert.Equal(1, activities.RightExecutions);
            var leftStep = await WorkflowStore.GetStepAsync(handle.WorkflowRunId, 0);
            var rightStep = await WorkflowStore.GetStepAsync(handle.WorkflowRunId, 1);
            Assert.Equal(WorkflowStepStatus.Succeeded, leftStep!.Status);
            Assert.Equal(WorkflowStepStatus.Succeeded, rightStep!.Status);
        }
        finally
        {
            activities.Release.TrySetResult();
            workflowWorkerCts.Cancel();
            activityWorkerCts.Cancel();
            await SwallowCancellation(workflowWorkerRun);
            await SwallowCancellation(activityWorkerRun);
        }
    }

    [Fact]
    public async Task Hosted_workflow_worker_terminally_fails_after_max_attempts()
    {
        var workflowRegistry = new WorkflowRegistry();
        workflowRegistry.Register<FailingWorkflow>();
        using var provider = new ServiceCollection().BuildServiceProvider();
        var runner = new WorkflowRunner(WorkflowStore, Store)
        {
            ActivityPollInterval = TimeSpan.FromMilliseconds(10),
            // Short backstop so any rare missed edge-trigger wake recovers well within test timeouts;
            // production keeps the longer default. The edge-trigger handles the common case.
            ParkGrace = TimeSpan.FromSeconds(2),
        };
        var client = new PgWorkflowClient(workflowRegistry, runner, provider);
        var worker = new WorkflowWorker(
            workflowRegistry,
            WorkflowStore,
            runner,
            provider,
            WorkflowWorkerOptions() with
            {
                MaxAttempts = 2,
                GetRetryDelay = _ => TimeSpan.Zero,
            }
        );
        var handle = await client.StartAsync<FailingWorkflow, string, string>("world");

        Assert.Equal(1, await worker.RunOnceAsync());

        var retryingRun = await WorkflowStore.GetRunAsync(handle.WorkflowRunId);
        Assert.Equal(WorkflowStatus.Pending, retryingRun!.Status);
        Assert.Equal(1, retryingRun.Attempt);
        Assert.Null(retryingRun.CompletedAt);
        Assert.Contains("boom world", retryingRun.Error);

        Assert.Equal(1, await worker.RunOnceAsync());

        var failedRun = await WorkflowStore.GetRunAsync(handle.WorkflowRunId);
        Assert.Equal(WorkflowStatus.Failed, failedRun!.Status);
        Assert.Equal(2, failedRun.Attempt);
        Assert.NotNull(failedRun.CompletedAt);
        Assert.Contains("boom world", failedRun.Error);
        Assert.Equal(0, await worker.RunOnceAsync());
    }

    [Fact]
    public async Task Hosted_workflow_worker_runs_failure_hooks_after_terminal_failure()
    {
        var activities = new FailureHookActivities();
        var registry = new ActivityRegistry();
        registry.Register("wf-hook-first", (string value) => activities.First(value));
        registry.Register("wf-hook-second", (string value) => activities.Second(value));
        var activityWorker = new ActivityWorker(registry, Store, Options("activity-worker"));
        var workflowRegistry = new WorkflowRegistry();
        workflowRegistry.Register<FailureHooksWorkflow>();
        using var provider = new ServiceCollection().BuildServiceProvider();
        var runner = new WorkflowRunner(WorkflowStore, Store)
        {
            ActivityPollInterval = TimeSpan.FromMilliseconds(10),
            // Short backstop so any rare missed edge-trigger wake recovers well within test timeouts;
            // production keeps the longer default. The edge-trigger handles the common case.
            ParkGrace = TimeSpan.FromSeconds(2),
        };
        var client = new PgWorkflowClient(workflowRegistry, runner, provider);
        var workflowWorker = new WorkflowWorker(
            workflowRegistry,
            WorkflowStore,
            runner,
            provider,
            WorkflowWorkerOptions() with
            {
                MaxAttempts = 2,
                GetRetryDelay = _ => TimeSpan.Zero,
            }
        );
        var handle = await client.StartAsync<FailureHooksWorkflow, string, string>("value");

        using var activityWorkerCts = new CancellationTokenSource();
        var activityWorkerRun = activityWorker.RunAsync(activityWorkerCts.Token);

        try
        {
            Assert.Equal(1, await workflowWorker.RunOnceAsync());
            Assert.Empty(activities.Executions);

            Assert.Equal(1, await workflowWorker.RunOnceAsync());

            var failedRun = await WorkflowStore.GetRunAsync(handle.WorkflowRunId);
            Assert.Equal(WorkflowStatus.Failed, failedRun!.Status);
            Assert.Equal(new[] { "second:value", "first:value" }, activities.Executions);

            var hooks = await WorkflowStore.ListFailureHooksAsync(handle.WorkflowRunId);
            Assert.Equal(2, hooks.Count);
            Assert.All(hooks, hook => Assert.Equal(WorkflowFailureHookStatus.Succeeded, hook.Status));
        }
        finally
        {
            activityWorkerCts.Cancel();
            await SwallowCancellation(activityWorkerRun);
        }
    }

    [Fact]
    public async Task Hosted_workflow_worker_reclaims_stale_unleased_running_run()
    {
        var workflowRegistry = new WorkflowRegistry();
        workflowRegistry.Register<ImmediateWorkflow>();
        using var provider = new ServiceCollection().BuildServiceProvider();
        var runner = new WorkflowRunner(WorkflowStore, Store)
        {
            ActivityPollInterval = TimeSpan.FromMilliseconds(10),
            // Short backstop so any rare missed edge-trigger wake recovers well within test timeouts;
            // production keeps the longer default. The edge-trigger handles the common case.
            ParkGrace = TimeSpan.FromSeconds(2),
        };
        var client = new PgWorkflowClient(workflowRegistry, runner, provider);
        var worker = new WorkflowWorker(
            workflowRegistry,
            WorkflowStore,
            runner,
            provider,
            WorkflowWorkerOptions() with { LeaseDuration = TimeSpan.FromMilliseconds(50) }
        );
        var handle = await client.StartAsync<ImmediateWorkflow, string, string>("world");
        await WorkflowStore.MarkRunRunningAsync(handle.WorkflowRunId);
        await MarkWorkflowRunStaleAsync(handle.WorkflowRunId);

        var processed = await worker.RunOnceAsync();

        Assert.Equal(1, processed);
        var run = await WorkflowStore.GetRunAsync(handle.WorkflowRunId);
        Assert.Equal(WorkflowStatus.Succeeded, run!.Status);
        Assert.Equal("done world", System.Text.Json.JsonSerializer.Deserialize<string>(run.ResultJson!));
    }

    [Fact]
    public async Task Hosted_workflow_worker_parks_sleeping_run_and_resumes_after_timer()
    {
        var activities = new SleepActivities();
        var registry = new ActivityRegistry();
        registry.Register("wf-sleep-first", (string value) => activities.First(value));
        registry.Register("wf-sleep-second", (string value) => activities.Second(value));
        var activityWorker = new ActivityWorker(registry, Store, Options("activity-worker"));
        var workflowRegistry = new WorkflowRegistry();
        workflowRegistry.Register<SleepWorkflow>();
        using var provider = new ServiceCollection().BuildServiceProvider();
        var runner = new WorkflowRunner(WorkflowStore, Store)
        {
            ActivityPollInterval = TimeSpan.FromMilliseconds(10),
            // Short backstop so any rare missed edge-trigger wake recovers well within test timeouts;
            // production keeps the longer default. The edge-trigger handles the common case.
            ParkGrace = TimeSpan.FromSeconds(2),
        };
        var client = new PgWorkflowClient(workflowRegistry, runner, provider);
        var worker = new WorkflowWorker(
            workflowRegistry,
            WorkflowStore,
            runner,
            provider,
            WorkflowWorkerOptions()
        );
        var handle = await client.StartAsync<SleepWorkflow, string, string>("value");

        using var activityWorkerCts = new CancellationTokenSource();
        var activityWorkerRun = activityWorker.RunAsync(activityWorkerCts.Token);
        using var workflowWorkerCts = new CancellationTokenSource();
        var workflowWorkerRun = worker.RunAsync(workflowWorkerCts.Token);

        try
        {
            // Drive the continuous workers until the run is durably parked on ctx.Sleep — i.e. its
            // timer row exists. Note the pre-sleep activity now also parks-and-resumes (every activity
            // wait parks), so we wait for the durable sleep state rather than counting worker passes.
            // The sleep is long (30s) so the checks below cannot race a real timer firing.
            var fireAt = await WaitForWorkflowTimerAsync(
                handle.WorkflowRunId,
                0,
                TimeSpan.FromSeconds(10)
            );

            var parked = await WorkflowStore.GetRunAsync(handle.WorkflowRunId);
            Assert.Equal(WorkflowStatus.Pending, parked!.Status);
            Assert.True(parked.VisibleAt > DateTimeOffset.UtcNow);
            Assert.Equal(parked.VisibleAt, fireAt);
            // The pre-sleep activity ran exactly once and its step is durably memoized; the post-sleep
            // activity has not run yet.
            Assert.Equal(1, activities.FirstExecutions);
            Assert.Equal(0, activities.SecondExecutions);
            var firstStepAfterPark = await WorkflowStore.GetStepAsync(handle.WorkflowRunId, 0);
            Assert.Equal(WorkflowStepStatus.Succeeded, firstStepAfterPark!.Status);

            // Fire the timer deterministically (no wall-clock wait): move both the timer's fire_at
            // and the run's visible_at into the past, simulating the deadline elapsing. The continuous
            // worker then resumes, replays the cached pre-sleep step, and runs the rest.
            await FireWorkflowTimerAsync(handle.WorkflowRunId, 0);

            // Wait via the store (not handle.GetResultAsync, which would re-execute inline in this
            // caller and cannot Sleep); read the durable result.
            var succeeded = await WaitForWorkflowTerminalAsync(
                handle.WorkflowRunId,
                TimeSpan.FromSeconds(10)
            );
            Assert.Equal(WorkflowStatus.Succeeded, succeeded.Status);
            Assert.Equal(
                "second:first:value",
                System.Text.Json.JsonSerializer.Deserialize<string>(succeeded.ResultJson!)
            );
            // End-to-end exactly-once: each activity body ran a single time across the park/resume
            // cycles (step memoization is the mechanism that guarantees it).
            Assert.Equal(1, activities.FirstExecutions);
            Assert.Equal(1, activities.SecondExecutions);
        }
        finally
        {
            workflowWorkerCts.Cancel();
            activityWorkerCts.Cancel();
            await SwallowCancellation(workflowWorkerRun);
            await SwallowCancellation(activityWorkerRun);
        }
    }

    [Fact]
    public async Task Hosted_workflow_worker_sleep_does_not_consume_retry_budget()
    {
        var workflowRegistry = new WorkflowRegistry();
        workflowRegistry.Register<SleepThenFailWorkflow>();
        using var provider = new ServiceCollection().BuildServiceProvider();
        var runner = new WorkflowRunner(WorkflowStore, Store)
        {
            ActivityPollInterval = TimeSpan.FromMilliseconds(10),
            // Short backstop so any rare missed edge-trigger wake recovers well within test timeouts;
            // production keeps the longer default. The edge-trigger handles the common case.
            ParkGrace = TimeSpan.FromSeconds(2),
        };
        var client = new PgWorkflowClient(workflowRegistry, runner, provider);
        var worker = new WorkflowWorker(
            workflowRegistry,
            WorkflowStore,
            runner,
            provider,
            WorkflowWorkerOptions() with
            {
                MaxAttempts = 2,
                GetRetryDelay = _ => TimeSpan.Zero,
            }
        );
        SleepThenFailWorkflow.Reset();
        var handle = await client.StartAsync<SleepThenFailWorkflow, string, string>("value");

        // First lease parks the run on the timer (this must not spend an attempt).
        Assert.Equal(1, await worker.RunOnceAsync());
        var parked = await WorkflowStore.GetRunAsync(handle.WorkflowRunId);
        Assert.Equal(WorkflowStatus.Pending, parked!.Status);

        // Fire the timer deterministically, then resume.
        await FireWorkflowTimerAsync(handle.WorkflowRunId, 0);

        // Resume: the post-sleep body fails. Because the sleep gave its attempt back, attempt 1 of 2
        // is the real failure, so the run is still retryable rather than terminal. (Remove the
        // greatest(attempt-1,0) decrement and this lands at attempt 2 -> terminal, failing here.)
        Assert.Equal(1, await worker.RunOnceAsync());
        var retrying = await WorkflowStore.GetRunAsync(handle.WorkflowRunId);
        Assert.Equal(WorkflowStatus.Pending, retrying!.Status);
        Assert.Contains("post-sleep transient failure", retrying.Error);

        // Final attempt succeeds (the fired timer stays elapsed, so it does not re-park).
        Assert.Equal(1, await worker.RunOnceAsync());
        var result = await handle.GetResultAsync();
        Assert.Equal("value", result);
        var succeeded = await WorkflowStore.GetRunAsync(handle.WorkflowRunId);
        Assert.Equal(WorkflowStatus.Succeeded, succeeded!.Status);
    }

    [Fact]
    public async Task Hosted_workflow_worker_fails_loudly_when_sleep_is_swallowed()
    {
        var workflowRegistry = new WorkflowRegistry();
        workflowRegistry.Register<SwallowsSleepWorkflow>();
        using var provider = new ServiceCollection().BuildServiceProvider();
        var runner = new WorkflowRunner(WorkflowStore, Store)
        {
            ActivityPollInterval = TimeSpan.FromMilliseconds(10),
            // Short backstop so any rare missed edge-trigger wake recovers well within test timeouts;
            // production keeps the longer default. The edge-trigger handles the common case.
            ParkGrace = TimeSpan.FromSeconds(2),
        };
        var client = new PgWorkflowClient(workflowRegistry, runner, provider);
        var worker = new WorkflowWorker(
            workflowRegistry,
            WorkflowStore,
            runner,
            provider,
            WorkflowWorkerOptions()
        );
        var handle = await client.StartAsync<SwallowsSleepWorkflow, string, string>("value");

        // The workflow catches the park exception and returns normally; the runner must fail the
        // run loudly rather than silently record success and skip the durable timer.
        Assert.Equal(1, await worker.RunOnceAsync());

        var run = await WorkflowStore.GetRunAsync(handle.WorkflowRunId);
        Assert.Equal(WorkflowStatus.Failed, run!.Status);
        Assert.Contains("swallowed", run.Error);
    }

    [Fact]
    public async Task Hosted_workflow_worker_fails_loudly_when_activity_park_is_swallowed()
    {
        var workflowRegistry = new WorkflowRegistry();
        workflowRegistry.Register<SwallowsActivityParkWorkflow>();
        using var provider = new ServiceCollection().BuildServiceProvider();
        var runner = new WorkflowRunner(WorkflowStore, Store)
        {
            ActivityPollInterval = TimeSpan.FromMilliseconds(10),
            ParkGrace = TimeSpan.FromSeconds(2),
        };
        var client = new PgWorkflowClient(workflowRegistry, runner, provider);
        var worker = new WorkflowWorker(
            workflowRegistry,
            WorkflowStore,
            runner,
            provider,
            WorkflowWorkerOptions()
        );
        var handle = await client.StartAsync<SwallowsActivityParkWorkflow, string, string>("value");

        // No activity worker runs, so the activity stays pending and the run tries to park. The
        // workflow swallows that control-flow exception and returns normally; the runner must fail
        // the run loudly rather than silently record success while the step never completed.
        Assert.Equal(1, await worker.RunOnceAsync());

        var run = await WorkflowStore.GetRunAsync(handle.WorkflowRunId);
        Assert.Equal(WorkflowStatus.Failed, run!.Status);
        Assert.Contains("swallowed", run.Error);
    }

    [Fact]
    public async Task Workflow_consumes_buffered_signals_fifo_and_dedupes_idempotency_key()
    {
        var workflowRegistry = new WorkflowRegistry();
        workflowRegistry.Register<BufferedSignalWorkflow>();
        using var provider = new ServiceCollection().BuildServiceProvider();
        var runner = new WorkflowRunner(WorkflowStore, Store)
        {
            ActivityPollInterval = TimeSpan.FromMilliseconds(10),
            ParkGrace = TimeSpan.FromSeconds(2),
        };
        var client = new PgWorkflowClient(workflowRegistry, runner, provider);
        var worker = new WorkflowWorker(
            workflowRegistry,
            WorkflowStore,
            runner,
            provider,
            WorkflowWorkerOptions()
        );
        var handle = await client.StartAsync<BufferedSignalWorkflow, string, string>("value");

        await client.SignalAsync(
            handle.WorkflowRunId,
            "approval",
            new SignalDecision("first"),
            idempotencyKey: "approval-1"
        );
        await client.SignalAsync(
            handle.WorkflowRunId,
            "approval",
            new SignalDecision("duplicate"),
            idempotencyKey: "approval-1"
        );
        await client.SignalAsync(
            handle.WorkflowRunId,
            "approval",
            new SignalDecision("second"),
            idempotencyKey: "approval-2"
        );

        Assert.Equal(1, await worker.RunOnceAsync());

        var result = await handle.GetResultAsync();
        Assert.Equal("first,second", result);
        Assert.Equal(2, await CountSignalsAsync(handle.WorkflowRunId));
    }

    [Fact]
    public async Task Workflow_wait_for_signal_parks_then_signal_wakes_and_resumes()
    {
        var workflowRegistry = new WorkflowRegistry();
        workflowRegistry.Register<SingleSignalWorkflow>();
        using var provider = new ServiceCollection().BuildServiceProvider();
        var runner = new WorkflowRunner(WorkflowStore, Store)
        {
            ActivityPollInterval = TimeSpan.FromMilliseconds(10),
            ParkGrace = TimeSpan.FromSeconds(30),
        };
        var client = new PgWorkflowClient(workflowRegistry, runner, provider);
        var worker = new WorkflowWorker(
            workflowRegistry,
            WorkflowStore,
            runner,
            provider,
            WorkflowWorkerOptions()
        );
        var handle = await client.StartAsync<SingleSignalWorkflow, string, string>("value");

        Assert.Equal(1, await worker.RunOnceAsync());

        var parked = await WorkflowStore.GetRunAsync(handle.WorkflowRunId);
        Assert.Equal(WorkflowStatus.Pending, parked!.Status);
        Assert.False(await RunLeaseHeldAsync(handle.WorkflowRunId));
        Assert.True(parked.VisibleAt > DateTimeOffset.UtcNow);

        await handle.SignalAsync("approval", new SignalDecision("approved"));

        var woken = await WorkflowStore.GetRunAsync(handle.WorkflowRunId);
        Assert.True(woken!.VisibleAt <= DateTimeOffset.UtcNow);

        Assert.Equal(1, await worker.RunOnceAsync());

        var result = await handle.GetResultAsync();
        Assert.Equal("approved", result);
        var succeeded = await WorkflowStore.GetRunAsync(handle.WorkflowRunId);
        Assert.Equal(WorkflowStatus.Succeeded, succeeded!.Status);
        Assert.Equal(1, succeeded.Attempt);
    }

    [Fact]
    public async Task Signal_during_sleep_is_buffered_without_waking_timer_early()
    {
        var workflowRegistry = new WorkflowRegistry();
        workflowRegistry.Register<SleepThenFailWorkflow>();
        using var provider = new ServiceCollection().BuildServiceProvider();
        var runner = new WorkflowRunner(WorkflowStore, Store)
        {
            ActivityPollInterval = TimeSpan.FromMilliseconds(10),
            ParkGrace = TimeSpan.FromSeconds(2),
        };
        var client = new PgWorkflowClient(workflowRegistry, runner, provider);
        var worker = new WorkflowWorker(
            workflowRegistry,
            WorkflowStore,
            runner,
            provider,
            WorkflowWorkerOptions()
        );
        SleepThenFailWorkflow.Reset();
        var handle = await client.StartAsync<SleepThenFailWorkflow, string, string>("value");

        Assert.Equal(1, await worker.RunOnceAsync());
        var sleeping = await WorkflowStore.GetRunAsync(handle.WorkflowRunId);
        Assert.Equal(WorkflowStatus.Pending, sleeping!.Status);
        Assert.True(sleeping.VisibleAt > DateTimeOffset.UtcNow);

        await client.SignalAsync(handle.WorkflowRunId, "approval", new SignalDecision("early"));

        var stillSleeping = await WorkflowStore.GetRunAsync(handle.WorkflowRunId);
        Assert.Equal(sleeping.VisibleAt, stillSleeping!.VisibleAt);
        Assert.Equal(0, await worker.RunOnceAsync());
        Assert.Equal(1, await CountSignalsAsync(handle.WorkflowRunId));
    }

    [Fact]
    public async Task Duplicate_signal_delivery_does_not_wake_parked_run()
    {
        var workflowRegistry = new WorkflowRegistry();
        workflowRegistry.Register<BufferedSignalWorkflow>();
        using var provider = new ServiceCollection().BuildServiceProvider();
        var runner = new WorkflowRunner(WorkflowStore, Store)
        {
            ActivityPollInterval = TimeSpan.FromMilliseconds(10),
        };
        var client = new PgWorkflowClient(workflowRegistry, runner, provider);
        var worker = new WorkflowWorker(
            workflowRegistry,
            WorkflowStore,
            runner,
            provider,
            WorkflowWorkerOptions()
        );
        var handle = await client.StartAsync<BufferedSignalWorkflow, string, string>("value");

        await handle.SignalAsync("approval", new SignalDecision("first"), idempotencyKey: "approval-1");
        Assert.Equal(1, await worker.RunOnceAsync());

        var parked = await WorkflowStore.GetRunAsync(handle.WorkflowRunId);
        Assert.Equal(WorkflowStatus.Pending, parked!.Status);
        Assert.True(parked.VisibleAt > DateTimeOffset.UtcNow);

        // Redelivering the already-consumed signal buffers nothing and must not wake the run.
        await handle.SignalAsync("approval", new SignalDecision("duplicate"), idempotencyKey: "approval-1");

        var stillParked = await WorkflowStore.GetRunAsync(handle.WorkflowRunId);
        Assert.Equal(parked.VisibleAt, stillParked!.VisibleAt);
        Assert.Equal(0, await worker.RunOnceAsync());
        Assert.Equal(1, await CountSignalsAsync(handle.WorkflowRunId));

        await handle.SignalAsync("approval", new SignalDecision("second"), idempotencyKey: "approval-2");
        Assert.Equal(1, await worker.RunOnceAsync());
        Assert.Equal("first,second", await handle.GetResultAsync());
    }

    private async Task FireWorkflowTimerAsync(Guid workflowRunId, int timerSequence)
    {
        var past = DateTimeOffset.UtcNow.Subtract(TimeSpan.FromSeconds(1));
        await using var command = DataSource.CreateCommand(
            """
            update pw_workflow_timers
            set fire_at = @past
            where workflow_run_id = @workflow_run_id and timer_seq = @timer_seq;

            update pw_workflow_runs
            set visible_at = @past
            where workflow_run_id = @workflow_run_id;
            """
        );
        command.Parameters.AddWithValue("workflow_run_id", workflowRunId);
        command.Parameters.AddWithValue("timer_seq", timerSequence);
        command.Parameters.AddWithValue("past", past);
        await command.ExecuteNonQueryAsync();
    }

    private async Task<WorkflowRun> SingleWorkflowRunAsync()
    {
        await using var command = DataSource.CreateCommand("select workflow_run_id from pw_workflow_runs;");
        var runId = (Guid)(await command.ExecuteScalarAsync())!;
        return (await WorkflowStore.GetRunAsync(runId))!;
    }

    private async Task<long> CountSignalsAsync(Guid workflowRunId)
    {
        await using var command = DataSource.CreateCommand(
            "select count(*) from pw_workflow_signals where workflow_run_id = @workflow_run_id;"
        );
        command.Parameters.AddWithValue("workflow_run_id", workflowRunId);
        return (long)(await command.ExecuteScalarAsync())!;
    }

    private async Task MarkWorkflowRunStaleAsync(Guid workflowRunId)
    {
        await using var command = DataSource.CreateCommand(
            """
            update pw_workflow_runs
            set updated_at = @updated_at
            where workflow_run_id = @workflow_run_id;
            """
        );
        command.Parameters.AddWithValue("workflow_run_id", workflowRunId);
        command.Parameters.AddWithValue("updated_at", DateTimeOffset.UtcNow.Subtract(TimeSpan.FromMinutes(1)));
        await command.ExecuteNonQueryAsync();
    }

    private async Task<WorkflowRun> WaitForWorkflowStatusAsync(
        Guid workflowRunId,
        WorkflowStatus status,
        TimeSpan timeout
    )
    {
        using var cts = new CancellationTokenSource(timeout);
        while (true)
        {
            cts.Token.ThrowIfCancellationRequested();

            var run = await WorkflowStore.GetRunAsync(workflowRunId, cts.Token);
            if (run is not null && run.Status == status)
            {
                return run;
            }

            await Task.Delay(10, cts.Token);
        }
    }

    /// <summary>Reads whether a workflow run currently holds a worker lease (lease_token not null).</summary>
    private async Task<bool> RunLeaseHeldAsync(Guid workflowRunId)
    {
        await using var command = DataSource.CreateCommand(
            "select lease_token from pw_workflow_runs where workflow_run_id = @id;"
        );
        command.Parameters.AddWithValue("id", workflowRunId);
        var leaseToken = await command.ExecuteScalarAsync();
        return leaseToken is not null and not DBNull;
    }

    private async Task<DateTimeOffset> WaitForWorkflowTimerAsync(
        Guid workflowRunId,
        int timerSequence,
        TimeSpan timeout
    )
    {
        using var cts = new CancellationTokenSource(timeout);
        while (true)
        {
            cts.Token.ThrowIfCancellationRequested();

            var fireAt = await WorkflowStore.GetTimerAsync(workflowRunId, timerSequence, cts.Token);
            if (fireAt is not null)
            {
                return fireAt.Value;
            }

            await Task.Delay(10, cts.Token);
        }
    }

    private async Task<WorkflowRun> WaitForWorkflowTerminalAsync(Guid workflowRunId, TimeSpan timeout)
    {
        using var cts = new CancellationTokenSource(timeout);
        while (true)
        {
            cts.Token.ThrowIfCancellationRequested();

            var run = await WorkflowStore.GetRunAsync(workflowRunId, cts.Token);
            if (run is { Status: WorkflowStatus.Succeeded or WorkflowStatus.Failed })
            {
                return run;
            }

            await Task.Delay(10, cts.Token);
        }
    }

    private IPgWorkflowClient CreateClient<TWorkflow>()
        where TWorkflow : class
    {
        var workflowRegistry = new WorkflowRegistry();
        workflowRegistry.Register<TWorkflow>();
        var services = new ServiceCollection();
        var provider = services.BuildServiceProvider();
        return new PgWorkflowClient(
            workflowRegistry,
            new WorkflowRunner(WorkflowStore, Store)
            {
                ActivityPollInterval = TimeSpan.FromMilliseconds(10),
            },
            provider
        );
    }

    private static ActivityWorkerOptions Options(string workerId) =>
        new()
        {
            WorkerId = workerId,
            BatchSize = 1,
            PollInterval = TimeSpan.FromMilliseconds(10),
        };

    private static WorkflowWorkerOptions WorkflowWorkerOptions() =>
        new()
        {
            WorkerId = "workflow-worker",
            BatchSize = 1,
            PollInterval = TimeSpan.FromMilliseconds(10),
        };

    private sealed class TestActivities
    {
        public int GreetExecutions => Volatile.Read(ref _greetExecutions);

        public int UppercaseExecutions => Volatile.Read(ref _uppercaseExecutions);

        private int _greetExecutions;
        private int _uppercaseExecutions;

        [Activity("wf-greet")]
        public string Greet(string name)
        {
            Interlocked.Increment(ref _greetExecutions);
            return $"hello {name}";
        }

        [Activity("wf-upper")]
        public string Uppercase(string value)
        {
            Interlocked.Increment(ref _uppercaseExecutions);
            return value.ToUpperInvariant();
        }
    }

    private sealed class FanOutActivities
    {
        public int EchoExecutions => Volatile.Read(ref _echoExecutions);

        public int JoinExecutions => Volatile.Read(ref _joinExecutions);

        public int MaxConcurrentEchoes => Volatile.Read(ref _maxConcurrentEchoes);

        private int _echoExecutions;
        private int _joinExecutions;
        private int _runningEchoes;
        private int _maxConcurrentEchoes;
        private readonly TaskCompletionSource _bothEchoesRunning = new(
            TaskCreationOptions.RunContinuationsAsynchronously
        );

        [Activity("wf-fanout-echo")]
        public async ValueTask<string> EchoAsync(string value, CancellationToken cancellationToken)
        {
            Interlocked.Increment(ref _echoExecutions);
            var running = Interlocked.Increment(ref _runningEchoes);
            UpdateMaxConcurrentEchoes(running);
            if (running == 2)
            {
                _bothEchoesRunning.TrySetResult();
            }

            try
            {
                await _bothEchoesRunning.Task.WaitAsync(
                    TimeSpan.FromSeconds(5),
                    cancellationToken
                );
                return value;
            }
            finally
            {
                Interlocked.Decrement(ref _runningEchoes);
            }
        }

        [Activity("wf-fanout-join")]
        public string Join(string value)
        {
            Interlocked.Increment(ref _joinExecutions);
            return value;
        }

        private void UpdateMaxConcurrentEchoes(int running)
        {
            while (true)
            {
                var current = Volatile.Read(ref _maxConcurrentEchoes);
                if (running <= current)
                {
                    return;
                }

                if (Interlocked.CompareExchange(ref _maxConcurrentEchoes, running, current) == current)
                {
                    return;
                }
            }
        }
    }

    private sealed class MixedFanOutActivities
    {
        public int OkExecutions => Volatile.Read(ref _okExecutions);

        public int FailExecutions => Volatile.Read(ref _failExecutions);

        private int _okExecutions;
        private int _failExecutions;

        [Activity("wf-mixed-ok")]
        public string Ok(string value)
        {
            Interlocked.Increment(ref _okExecutions);
            return value;
        }

        [Activity("wf-mixed-fail")]
        public string Fail(string value)
        {
            Interlocked.Increment(ref _failExecutions);
            throw new InvalidOperationException($"boom {value}");
        }
    }

    private sealed class RetryActivities
    {
        public int EchoExecutions => Volatile.Read(ref _echoExecutions);

        private int _echoExecutions;

        [Activity("wf-retry-echo")]
        public string Echo(string value)
        {
            Interlocked.Increment(ref _echoExecutions);
            return value;
        }
    }

    private sealed class FailureHookActivities
    {
        private readonly List<string> _executions = [];
        private readonly object _lock = new();

        public IReadOnlyList<string> Executions
        {
            get
            {
                lock (_lock)
                {
                    return _executions.ToArray();
                }
            }
        }

        [Activity("wf-hook-first")]
        public string First(string value)
        {
            lock (_lock)
            {
                _executions.Add($"first:{value}");
            }

            return value;
        }

        [Activity("wf-hook-second")]
        public string Second(string value)
        {
            lock (_lock)
            {
                _executions.Add($"second:{value}");
            }

            return value;
        }
    }

    private sealed class SleepActivities
    {
        public int FirstExecutions => Volatile.Read(ref _firstExecutions);

        public int SecondExecutions => Volatile.Read(ref _secondExecutions);

        private int _firstExecutions;
        private int _secondExecutions;

        [Activity("wf-sleep-first")]
        public string First(string value)
        {
            Interlocked.Increment(ref _firstExecutions);
            return $"first:{value}";
        }

        [Activity("wf-sleep-second")]
        public string Second(string value)
        {
            Interlocked.Increment(ref _secondExecutions);
            return $"second:{value}";
        }
    }

    private sealed class GatedActivities
    {
        public int Executions => Volatile.Read(ref _executions);

        public TaskCompletionSource Started { get; } =
            new(TaskCreationOptions.RunContinuationsAsynchronously);

        public TaskCompletionSource Release { get; } =
            new(TaskCreationOptions.RunContinuationsAsynchronously);

        private int _executions;

        [Activity("wf-gated")]
        public async ValueTask<string> RunAsync(string value, CancellationToken cancellationToken)
        {
            Interlocked.Increment(ref _executions);
            Started.TrySetResult();
            await Release.Task.WaitAsync(cancellationToken);
            return $"gated:{value}";
        }
    }

    [Workflow]
    public sealed class GatedActivityWorkflow
    {
        [WorkflowRun]
        public ValueTask<string> RunAsync(
            IWorkflowContext ctx,
            string input,
            CancellationToken cancellationToken
        ) => ctx.Activity((GatedActivities a) => a.RunAsync(input, cancellationToken), cancellationToken);
    }

    private sealed class GatedFanOutActivities
    {
        public int LeftExecutions => Volatile.Read(ref _leftExecutions);

        public int RightExecutions => Volatile.Read(ref _rightExecutions);

        public TaskCompletionSource LeftStarted { get; } =
            new(TaskCreationOptions.RunContinuationsAsynchronously);

        public TaskCompletionSource RightStarted { get; } =
            new(TaskCreationOptions.RunContinuationsAsynchronously);

        public TaskCompletionSource Release { get; } =
            new(TaskCreationOptions.RunContinuationsAsynchronously);

        private int _leftExecutions;
        private int _rightExecutions;

        [Activity("wf-gated-left")]
        public async ValueTask<string> LeftAsync(string value, CancellationToken cancellationToken)
        {
            Interlocked.Increment(ref _leftExecutions);
            LeftStarted.TrySetResult();
            await Release.Task.WaitAsync(cancellationToken);
            return $"L:{value}";
        }

        [Activity("wf-gated-right")]
        public async ValueTask<string> RightAsync(string value, CancellationToken cancellationToken)
        {
            Interlocked.Increment(ref _rightExecutions);
            RightStarted.TrySetResult();
            await Release.Task.WaitAsync(cancellationToken);
            return $"R:{value}";
        }
    }

    [Workflow]
    public sealed class GatedFanOutWorkflow
    {
        [WorkflowRun]
        public async ValueTask<string> RunAsync(
            IWorkflowContext ctx,
            string input,
            CancellationToken cancellationToken
        )
        {
            var (left, right) = await ctx.WhenAll(
                ctx.CallActivity((GatedFanOutActivities a) => a.LeftAsync(input, cancellationToken)),
                ctx.CallActivity((GatedFanOutActivities a) => a.RightAsync(input, cancellationToken)),
                cancellationToken
            );

            return $"{left} {right}";
        }
    }

    [Workflow]
    public sealed class SleepWorkflow
    {
        [WorkflowRun]
        public async ValueTask<string> RunAsync(
            IWorkflowContext ctx,
            string input,
            CancellationToken cancellationToken
        )
        {
            var first = await ctx.Activity(
                (SleepActivities a) => a.First(input),
                cancellationToken
            );

            // Long sleep: the test fires the timer deterministically rather than waiting it out.
            await ctx.Sleep(TimeSpan.FromSeconds(30), cancellationToken);

            return await ctx.Activity(
                (SleepActivities a) => a.Second(first),
                cancellationToken
            );
        }
    }

    [Workflow]
    public sealed class SleepThenFailWorkflow
    {
        private static int s_invocations;

        public static void Reset() => Volatile.Write(ref s_invocations, 0);

        [WorkflowRun]
        public async ValueTask<string> RunAsync(
            IWorkflowContext ctx,
            string input,
            CancellationToken cancellationToken
        )
        {
            await ctx.Sleep(TimeSpan.FromSeconds(30), cancellationToken);

            if (Interlocked.Increment(ref s_invocations) == 1)
            {
                throw new InvalidOperationException("post-sleep transient failure");
            }

            return input;
        }
    }

    [Workflow]
    public sealed class SwallowsSleepWorkflow
    {
        [WorkflowRun]
        public async ValueTask<string> RunAsync(
            IWorkflowContext ctx,
            string input,
            CancellationToken cancellationToken
        )
        {
            try
            {
                await ctx.Sleep(TimeSpan.FromSeconds(30), cancellationToken);
            }
            catch (Exception)
            {
                // Intentionally swallow the park exception to exercise the fail-loud guard.
            }

            return input;
        }
    }

    [Workflow]
    public sealed class SwallowsActivityParkWorkflow
    {
        [WorkflowRun]
        public async ValueTask<string> RunAsync(
            IWorkflowContext ctx,
            string input,
            CancellationToken cancellationToken
        )
        {
            try
            {
                await ctx.Activity((TestActivities a) => a.Greet(input), cancellationToken);
            }
            catch (Exception)
            {
                // Intentionally swallow the activity park exception to exercise the fail-loud guard.
            }

            return input;
        }
    }

    public sealed record FanOutInput(string Left, string Right);

    public sealed record SignalDecision(string Value);

    [Workflow]
    public sealed class SingleSignalWorkflow
    {
        [WorkflowRun]
        public async ValueTask<string> RunAsync(
            IWorkflowContext ctx,
            string input,
            CancellationToken cancellationToken
        )
        {
            var decision = await ctx.WaitForSignal<SignalDecision>("approval", cancellationToken);
            return decision.Value;
        }
    }

    [Workflow]
    public sealed class BufferedSignalWorkflow
    {
        [WorkflowRun]
        public async ValueTask<string> RunAsync(
            IWorkflowContext ctx,
            string input,
            CancellationToken cancellationToken
        )
        {
            var first = await ctx.WaitForSignal<SignalDecision>("approval", cancellationToken);
            var second = await ctx.WaitForSignal<SignalDecision>("approval", cancellationToken);
            return $"{first.Value},{second.Value}";
        }
    }

    [Workflow]
    public sealed class ClientGreetingWorkflow
    {
        [WorkflowRun]
        public async ValueTask<string> RunAsync(
            IWorkflowContext ctx,
            string name,
            CancellationToken cancellationToken
        )
        {
            var greeting = await ctx.Activity(
                (TestActivities a) => a.Greet(name),
                cancellationToken
            );
            return await ctx.Activity(
                (TestActivities a) => a.Uppercase(greeting),
                cancellationToken
            );
        }
    }

    [Workflow]
    public sealed class ResumableGreetingWorkflow
    {
        private static CancellationTokenSource? s_cancelAfterFirstStep;

        public static void Reset(CancellationTokenSource? cancelAfterFirstStep) =>
            s_cancelAfterFirstStep = cancelAfterFirstStep;

        [WorkflowRun]
        public async ValueTask<string> RunAsync(
            IWorkflowContext ctx,
            string name,
            CancellationToken cancellationToken
        )
        {
            var greeting = await ctx.Activity(
                (TestActivities a) => a.Greet(name),
                cancellationToken
            );

            if (s_cancelAfterFirstStep is not null)
            {
                await s_cancelAfterFirstStep.CancelAsync();
                cancellationToken.ThrowIfCancellationRequested();
            }

            return await ctx.Activity(
                (TestActivities a) => a.Uppercase(greeting),
                cancellationToken
            );
        }
    }

    [Workflow]
    public sealed class ResumableFanOutWorkflow
    {
        private static CancellationTokenSource? s_cancelAfterFanOut;

        public static void Reset(CancellationTokenSource? cancelAfterFanOut) =>
            s_cancelAfterFanOut = cancelAfterFanOut;

        [WorkflowRun]
        public async ValueTask<string> RunAsync(
            IWorkflowContext ctx,
            FanOutInput input,
            CancellationToken cancellationToken
        )
        {
            var (left, right) = await ctx.WhenAll(
                ctx.CallActivity((FanOutActivities a) => a.EchoAsync(input.Left, cancellationToken)),
                ctx.CallActivity((FanOutActivities a) => a.EchoAsync(input.Right, cancellationToken)),
                cancellationToken
            );

            if (s_cancelAfterFanOut is not null)
            {
                await s_cancelAfterFanOut.CancelAsync();
                cancellationToken.ThrowIfCancellationRequested();
            }

            return await ctx.Activity(
                (FanOutActivities a) => a.Join($"{left} {right}"),
                cancellationToken
            );
        }
    }

    [Workflow]
    public sealed class MixedFailureFanOutWorkflow
    {
        [WorkflowRun]
        public async ValueTask<string> RunAsync(
            IWorkflowContext ctx,
            string input,
            CancellationToken cancellationToken
        )
        {
            var (ok, failed) = await ctx.WhenAll(
                ctx.CallActivity((MixedFanOutActivities a) => a.Ok(input)),
                ctx.CallActivity((MixedFanOutActivities a) => a.Fail(input)),
                cancellationToken
            );

            return $"{ok} {failed}";
        }
    }

    [Workflow]
    public sealed class RetryAfterActivityWorkflow
    {
        private static int s_invocations;

        public static void Reset() => Volatile.Write(ref s_invocations, 0);

        [WorkflowRun]
        public async ValueTask<string> RunAsync(
            IWorkflowContext ctx,
            string input,
            CancellationToken cancellationToken
        )
        {
            var value = await ctx.Activity(
                (RetryActivities a) => a.Echo(input),
                cancellationToken
            );

            if (Interlocked.Increment(ref s_invocations) == 1)
            {
                throw new InvalidOperationException("workflow transient failure");
            }

            return value;
        }
    }

    [Workflow]
    public sealed class FailureHooksWorkflow
    {
        [WorkflowRun]
        public async ValueTask<string> RunAsync(
            IWorkflowContext ctx,
            string input,
            CancellationToken cancellationToken
        )
        {
            await ctx.OnFailure(
                (FailureHookActivities a) => a.First(input),
                cancellationToken
            );
            await ctx.OnFailure(
                (FailureHookActivities a) => a.Second(input),
                cancellationToken
            );
            throw new InvalidOperationException($"boom {input}");
        }
    }

    [Workflow("duplicate-workflow-name")]
    public sealed class DuplicateNameWorkflowA
    {
        [WorkflowRun]
        public string Run(IWorkflowContext _, string input) => input;
    }

    [Workflow("duplicate-workflow-name")]
    public sealed class DuplicateNameWorkflowB
    {
        [WorkflowRun]
        public string Run(IWorkflowContext _, string input) => input;
    }

    [Workflow]
    public sealed class CustomNamedWorkflow
    {
        [WorkflowRun]
        public ValueTask<string> RunAsync(
            IWorkflowContext ctx,
            string name,
            CancellationToken cancellationToken
        ) =>
            ctx.Activity((CustomNamedActivities a) => a.Greet(name), cancellationToken);
    }

    public sealed class CustomNamedActivities
    {
        public int Executions => Volatile.Read(ref _executions);

        private int _executions;

        [Activity("custom-greet")]
        public string Greet(string name)
        {
            Interlocked.Increment(ref _executions);
            return $"custom {name}";
        }
    }

    [Workflow]
    public sealed class FailingWorkflow
    {
        [WorkflowRun]
        public string Run(IWorkflowContext _, string name) =>
            throw new InvalidOperationException($"boom {name}");
    }

    [Workflow]
    public sealed class TransientInfrastructureFailureWorkflow
    {
        private static int s_invocations;

        public static void Reset() => Volatile.Write(ref s_invocations, 0);

        [WorkflowRun]
        public string Run(IWorkflowContext _, string name)
        {
            if (Interlocked.Increment(ref s_invocations) == 1)
            {
                throw new PostgresException(
                    "sorry, too many clients already",
                    "FATAL",
                    "FATAL",
                    "53300"
                );
            }

            return $"hello {name}";
        }
    }

    [Workflow]
    public sealed class ImmediateWorkflow
    {
        [WorkflowRun]
        public string Run(IWorkflowContext _, string name) => $"done {name}";
    }

}
