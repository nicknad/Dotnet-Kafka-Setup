namespace KafkaPipeline.Core.Messaging;

public static class DeadLetterHeaders
{
    public const string OriginalTopic = "dlq.original.topic";

    public const string OriginalPartition = "dlq.original.partition";

    public const string OriginalOffset = "dlq.original.offset";

    public const string ErrorReason = "dlq.error.reason";

    public const string ErrorTimestamp = "dlq.error.timestamp";
}
