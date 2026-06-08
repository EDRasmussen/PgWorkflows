using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
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
    public async Task Hosted_workflow_worker_treats_recorded_workflow_failure_as_processed()
    {
        var workflowRegistry = new WorkflowRegistry();
        workflowRegistry.Register<FailingWorkflow>();
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
        var handle = await client.StartAsync<FailingWorkflow, string, string>("world");

        var processed = await worker.RunOnceAsync();

        Assert.Equal(1, processed);
        var run = await WorkflowStore.GetRunAsync(handle.WorkflowRunId);
        Assert.Equal(WorkflowStatus.Failed, run!.Status);
        Assert.Contains("boom world", run.Error);
        Assert.Equal(0, await worker.RunOnceAsync());
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

        try
        {
            Assert.Equal(1, await workflowWorker.RunOnceAsync());

            var retryingRun = await WorkflowStore.GetRunAsync(handle.WorkflowRunId);
            Assert.Equal(WorkflowStatus.Pending, retryingRun!.Status);
            Assert.Equal(1, retryingRun.Attempt);
            Assert.Equal(2, retryingRun.MaxAttempts);
            Assert.Equal(1, activities.EchoExecutions);

            Assert.Equal(1, await workflowWorker.RunOnceAsync());

            var result = await handle.GetResultAsync();
            var succeededRun = await WorkflowStore.GetRunAsync(handle.WorkflowRunId);
            Assert.Equal("value", result);
            Assert.Equal(WorkflowStatus.Succeeded, succeededRun!.Status);
            Assert.Equal(2, succeededRun.Attempt);
            Assert.Equal(1, activities.EchoExecutions);
        }
        finally
        {
            activityWorkerCts.Cancel();
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

    private async Task<WorkflowRun> SingleWorkflowRunAsync()
    {
        await using var command = DataSource.CreateCommand("select workflow_run_id from pw_workflow_runs;");
        var runId = (Guid)(await command.ExecuteScalarAsync())!;
        return (await WorkflowStore.GetRunAsync(runId))!;
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

    public sealed record FanOutInput(string Left, string Right);

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
    public sealed class ImmediateWorkflow
    {
        [WorkflowRun]
        public string Run(IWorkflowContext _, string name) => $"done {name}";
    }

}
