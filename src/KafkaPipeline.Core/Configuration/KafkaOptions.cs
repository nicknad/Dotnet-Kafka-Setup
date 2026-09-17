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
}
