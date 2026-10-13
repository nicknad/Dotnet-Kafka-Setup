namespace KafkaPipeline.Core.Configuration;

public sealed class KafkaOptions
{
    public const string SectionName = "Kafka";

    public string BootstrapServers { get; set; } = string.Empty;

    public string Topic { get; set; } = string.Empty;

    public string ConsumerGroup { get; set; } = string.Empty;

    public string DeadLetterTopic { get; set; } = string.Empty;

    public string RetryTopic { get; set; } = string.Empty;

    public int MaxRetryAttempts { get; set; } = 3;

    public int RetryBackoffMs { get; set; } = 1000;

    public int CommitBatchSize { get; set; } = 100;

    public TimeSpan CommitInterval { get; set; } = TimeSpan.FromSeconds(5);

    /// <summary>
    /// Maximum number of processed message keys retained for consumer
    /// deduplication. Bounds the memory of the in-memory idempotency store;
    /// oldest keys are evicted first. Must be at least 1.
    /// </summary>
    public int IdempotencyMaxEntries { get; set; } = 100_000;

    /// <summary>
    /// Which idempotency store the consumer uses. <see cref="IdempotencyStoreKind.RocksDb"/>
    /// (default) survives restarts; <see cref="IdempotencyStoreKind.InMemory"/> is
    /// process-local and only deduplicates within a single run.
    /// </summary>
    public IdempotencyStoreKind IdempotencyStore { get; set; } = IdempotencyStoreKind.RocksDb;

    /// <summary>
    /// Directory of the RocksDB idempotency store. Empty uses a per-machine
    /// temporary directory; set to a mounted volume for durability across
    /// container restarts.
    /// </summary>
    public string IdempotencyStorePath { get; set; } = string.Empty;
}
