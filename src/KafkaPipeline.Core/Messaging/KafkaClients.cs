using Confluent.Kafka;

namespace KafkaPipeline.Core.Messaging;

public static class KafkaClients
{
    public static ProducerConfig IdempotentProducerConfig(string bootstrapServers) => new()
    {
        BootstrapServers = bootstrapServers,
        Acks = Acks.All,
        EnableIdempotence = true
    };
}
