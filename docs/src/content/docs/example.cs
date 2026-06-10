[Workflow("trial-onboarding")]
public sealed class TrialOnboardingWorkflow
{
    [WorkflowRun]
    public async ValueTask<string> RunAsync(
        IWorkflowContext ctx,
        SignupInput input,
        CancellationToken cancellationToken
    )
    {
        // Fan-out: run independent activities in parallel.
        var (workspace, _) = await ctx.WhenAll(
            ctx.CallActivity((OnboardingActivities a) => a.ProvisionWorkspace(input.Company)),
            ctx.CallActivity((EmailActivities a) => a.SendWelcome(input.Email)),
            cancellationToken
        );

        // Durable timer: the run is parked in Postgres — it survives
        // restarts and deploys, and no worker holds it in memory.
        await ctx.Sleep(TimeSpan.FromDays(11), cancellationToken);

        await ctx.Activity(
            (EmailActivities a) => a.SendTrialEndingReminder(input.Email),
            cancellationToken
        );

        // Human-in-the-loop: park again until an external signal arrives.
        var decision = await ctx.WaitForSignal<UpgradeDecision>("upgrade", cancellationToken);

        if (!decision.Upgraded)
        {
            await ctx.Activity(
                (OnboardingActivities a) => a.DowngradeToFreeTier(workspace.Id),
                cancellationToken
            );
            return $"{input.Company} stayed on the free tier.";
        }

        await ctx.Activity(
            (BillingActivities a) => a.StartSubscription(workspace.Id, decision.Plan),
            cancellationToken
        );
        return $"{input.Company} upgraded to {decision.Plan}.";
    }
}

public sealed record SignupInput(string Company, string Email);

public sealed record UpgradeDecision(bool Upgraded, string Plan);
