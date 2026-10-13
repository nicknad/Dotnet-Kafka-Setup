namespace KafkaPipeline.Core.Messaging;

public static class RetryHeaders
{
    public const string Count = "retry.count";

    public const string FirstTimestamp = "retry.first-timestamp";

    public const string LastError = "retry.last-error";
}
