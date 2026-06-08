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

internal sealed record ReserveInventoryInput(string UserName, string ItemName);

internal sealed record InventoryReservation(string ReservationId, string UserName, string ItemName);

internal sealed record ReleaseInventoryInput(
    string ReservationId,
    string UserName,
    string ItemName
);

internal sealed record ChargePaymentInput(string UserName, decimal Amount);

internal sealed record PaymentReceipt(string PaymentId, string UserName, decimal Amount);

internal sealed record RefundPaymentInput(string PaymentId, string UserName, decimal Amount);

internal sealed record CreateShipmentInput(string UserName, string ItemName, string ReservationId);

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
                activities.ReserveInventory(new ReserveInventoryInput(input.UserName, itemName)),
            cancellationToken
        );

        await ctx.OnFailure(
            (CheckoutActivities activities) =>
                activities.ReleaseInventory(
                    new ReleaseInventoryInput(
                        reservation.ReservationId,
                        reservation.UserName,
                        reservation.ItemName
                    )
                ),
            cancellationToken
        );

        var payment = await ctx.Activity(
            (CheckoutActivities activities) =>
                activities.ChargePayment(new ChargePaymentInput(input.UserName, input.Amount)),
            cancellationToken
        );

        await ctx.OnFailure(
            (CheckoutActivities activities) =>
                activities.RefundPayment(
                    new RefundPaymentInput(payment.PaymentId, payment.UserName, payment.Amount)
                ),
            cancellationToken
        );

        await ctx.Activity(
            (CheckoutActivities activities) =>
                activities.CreateShipment(
                    new CreateShipmentInput(input.UserName, itemName, reservation.ReservationId)
                ),
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
    public InventoryReservation ReserveInventory(ReserveInventoryInput input)
    {
        var reservationId = $"res-{Guid.NewGuid():N}";
        Console.WriteLine($"Reserved {input.ItemName} for {input.UserName} ({reservationId}).");
        return new InventoryReservation(reservationId, input.UserName, input.ItemName);
    }

    [Activity("release-inventory")]
    public void ReleaseInventory(ReleaseInventoryInput input) =>
        Console.WriteLine(
            $"Released {input.ItemName} reservation for {input.UserName} ({input.ReservationId})."
        );

    [Activity("charge-payment")]
    public PaymentReceipt ChargePayment(ChargePaymentInput input)
    {
        var paymentId = $"pay-{Guid.NewGuid():N}";
        Console.WriteLine($"{input.UserName} paid ${input.Amount:0.##}! ({paymentId})");
        return new PaymentReceipt(paymentId, input.UserName, input.Amount);
    }

    [Activity("refund-payment")]
    public void RefundPayment(RefundPaymentInput input) =>
        Console.WriteLine(
            $"Refunded ${input.Amount:0.##} to {input.UserName} ({input.PaymentId})."
        );

    [Activity("create-shipment")]
    public string CreateShipment(CreateShipmentInput input)
    {
        Console.WriteLine($"Creating shipment for {input.UserName}'s {input.ItemName}...");
        throw new InvalidOperationException("Warehouse label printer is offline.");
    }
}
