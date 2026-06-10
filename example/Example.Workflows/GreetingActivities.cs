using PgWorkflows.Activities;

namespace Example.Workflows;

public sealed class GreetingActivities
{
    [Activity("compose-greeting")]
    public string ComposeGreeting(string name) =>
        $"Hello, {(string.IsNullOrWhiteSpace(name) ? "world" : name)}!";

    [Activity("deliver-greeting")]
    public string DeliverGreeting(string greeting)
    {
        Console.WriteLine($"[worker] {greeting}");
        return greeting;
    }
}
