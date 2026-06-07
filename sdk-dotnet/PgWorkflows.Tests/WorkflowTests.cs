using Microsoft.Extensions.DependencyInjection;
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

    private async Task<WorkflowRun> SingleWorkflowRunAsync()
    {
        await using var command = DataSource.CreateCommand("select workflow_run_id from pw_workflow_runs;");
        var runId = (Guid)(await command.ExecuteScalarAsync())!;
        return (await WorkflowStore.GetRunAsync(runId))!;
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

}
