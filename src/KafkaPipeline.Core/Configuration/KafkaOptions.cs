namespace KafkaPipeline.Core.Configuration;

public sealed class KafkaOptions
{
    public const string SectionName = "Kafka";

    public string BootstrapServers { get; set; } = string.Empty;

    public string Topic { get; set; } = string.Empty;

    public string ConsumerGroup { get; set; } = string.Empty;

    public string DeadLetterTopic { get; set; } = string.Empty;

    public int CommitBatchSize { get; set; } = 100;

    public TimeSpan CommitInterval { get; set; } = TimeSpan.FromSeconds(5);

    /// <summary>
    /// Maximum number of processed message keys retained for consumer
    /// deduplication. Bounds the memory of the in-memory idempotency store;
    /// oldest keys are evicted first. Must be at least 1.
    /// </summary>
    public int IdempotencyMaxEntries { get; set; } = 100_000;
}
