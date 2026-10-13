using Confluent.Kafka;

namespace KafkaPipeline.Core.Messaging;

public static class KafkaClients
{
    /// <summary>
    /// How often librdkafka emits statistics callbacks (which feed lag /
    /// broker-connectivity logging). Zero disables them.
    /// </summary>
    public const int StatisticsIntervalMs = 30_000;

    public static ProducerConfig IdempotentProducerConfig(string bootstrapServers) => new()
    {
        BootstrapServers = bootstrapServers,
        Acks = Acks.All,
        EnableIdempotence = true,
        StatisticsIntervalMs = StatisticsIntervalMs
    };
}
