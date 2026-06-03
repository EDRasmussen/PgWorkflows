namespace PgWorkflows.Activities;

public static class ActivityName
{
    public static string For<TActivity>() where TActivity : IActivity => For(typeof(TActivity));

    public static string For(Type activityType)
    {
        ArgumentNullException.ThrowIfNull(activityType);
        return activityType.FullName ?? activityType.Name;
    }
}
