using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using PgWorkflows;
using PgWorkflows.Activities;
using PgWorkflows.Workflows;

var connectionString = Environment.GetEnvironmentVariable("PGWORKFLOWS_CONNECTION_STRING");
if (string.IsNullOrWhiteSpace(connectionString))
{
    throw new InvalidOperationException(
        "Set the PGWORKFLOWS_CONNECTION_STRING environment variable before running the sample."
    );
}

var builder = Host.CreateApplicationBuilder(args);

builder.Services.AddPgWorkflows(pg =>
    pg.UsePostgres(connectionString)
        .ConfigureWorkflowWorker(options =>
            options with
            {
                WorkerId = "console-sample-workflows",
                BatchSize = 1,
                PollInterval = TimeSpan.FromMilliseconds(100),
            }
        )
        .AddWorkflow<GreetingWorkflow>()
        .AddWorkflow<CheckoutWorkflow>()
        .AddActivities<HelloActivities>()
        .AddActivities<CheckoutActivities>()
);

using var app = builder.Build();
await app.StartAsync();

try
{
    var workflows = app.Services.GetRequiredService<IPgWorkflowClient>();
    var handle = await workflows.StartAsync<GreetingWorkflow, GreetingWorkflowInput, string>(
        new GreetingWorkflowInput("Postgres", 42),
        idempotencyKey: "console-sample"
    );
    var result = await handle.GetResultAsync();

    Console.WriteLine($"Workflow run id: {handle.WorkflowRunId}");
    Console.WriteLine($"Workflow result: {result}");

    var checkoutHandle = await workflows.StartAsync<CheckoutWorkflow, CheckoutInput, string>(
        new CheckoutInput("Ada", 100m)
    );

    try
    {
        await checkoutHandle.GetResultAsync();
    }
    catch (InvalidOperationException ex)
    {
        Console.WriteLine($"Checkout workflow run id: {checkoutHandle.WorkflowRunId}");
        Console.WriteLine($"Checkout failed as expected: {FirstLine(ex.Message)}");
    }
}
finally
{
    await app.StopAsync();
}

static string FirstLine(string message) =>
    message.Split(Environment.NewLine, StringSplitOptions.None)[0];

internal sealed record GreetingWorkflowInput(string Name, int GoodbyeId);

internal sealed record CheckoutInput(string UserName, decimal Amount);

internal sealed record InventoryReservation(string ReservationId, string UserName, string ItemName);

internal sealed record ChargePaymentInput(string UserName, decimal Amount);

internal sealed record PaymentReceipt(string PaymentId, string UserName, decimal Amount);

[Workflow("console-sample-workflow")]
internal sealed class GreetingWorkflow
{
    [WorkflowRun]
    public async ValueTask<string> RunAsync(
        IWorkflowContext ctx,
        GreetingWorkflowInput input,
        CancellationToken cancellationToken
    )
    {
        var (hello, goodbye) = await ctx.WhenAll(
            ctx.CallActivity((HelloActivities activities) => activities.Hello(input.Name)),
            ctx.CallActivity((HelloActivities activities) => activities.Goodbye(input.GoodbyeId)),
            cancellationToken
        );

        return $"{hello} {goodbye}";
    }
}

[Workflow("console-sample-checkout-workflow")]
internal sealed class CheckoutWorkflow
{
    [WorkflowRun]
    public async ValueTask<string> RunAsync(
        IWorkflowContext ctx,
        CheckoutInput input,
        CancellationToken cancellationToken
    )
    {
        const string itemName = "PgWorkflows hoodie";

        var reservation = await ctx.Activity(
            (CheckoutActivities activities) =>
                activities.ReserveInventory(input.UserName, itemName),
            cancellationToken
        );

        await ctx.OnFailure(
            (CheckoutActivities activities) =>
                activities.ReleaseInventory(
                    reservation.ReservationId,
                    reservation.UserName,
                    reservation.ItemName
                ),
            cancellationToken
        );

        var payment = await ctx.Activity(
            (CheckoutActivities activities) =>
                activities.ChargePayment(input.UserName, input.Amount),
            cancellationToken
        );

        await ctx.OnFailure(
            (CheckoutActivities activities) =>
                activities.RefundPayment(payment.Amount, payment.UserName, payment.PaymentId),
            cancellationToken
        );

        await ctx.Activity(
            (CheckoutActivities activities) => activities.CreateShipment(input.UserName, itemName),
            cancellationToken
        );

        return "Checkout completed.";
    }
}

internal sealed class HelloActivities
{
    [Activity("hello")]
    public string Hello(string name) =>
        $"Hello, {(string.IsNullOrWhiteSpace(name) ? "world" : name)}.";

    [Activity("goodbye")]
    public string Goodbye(int id) => $"Goodbye, {id}.";
}

internal sealed class CheckoutActivities
{
    [Activity("reserve-inventory")]
    public InventoryReservation ReserveInventory(string userName, string itemName)
    {
        var reservationId = $"res-{Guid.NewGuid():N}";
        Console.WriteLine($"Reserved {itemName} for {userName} ({reservationId}).");
        return new InventoryReservation(reservationId, userName, itemName);
    }

    [Activity("release-inventory")]
    public void ReleaseInventory(string reservationId, string userName, string itemName) =>
        Console.WriteLine($"Released {itemName} reservation for {userName} ({reservationId}).");

    [Activity("charge-payment")]
    public PaymentReceipt ChargePayment(string userName, decimal amount)
    {
        var paymentId = $"pay-{Guid.NewGuid():N}";
        Console.WriteLine($"{userName} paid ${amount:0.##}! ({paymentId})");
        return new PaymentReceipt(paymentId, userName, amount);
    }

    [Activity("refund-payment")]
    public void RefundPayment(decimal amount, string userName, string paymentId) =>
        Console.WriteLine($"Refunded ${amount:0.##} to {userName} ({paymentId}).");

    [Activity("create-shipment")]
    public string CreateShipment(string userName, string itemName)
    {
        Console.WriteLine($"Creating shipment for {userName}'s {itemName}...");
        throw new InvalidOperationException("Warehouse label printer is offline.");
    }
}
