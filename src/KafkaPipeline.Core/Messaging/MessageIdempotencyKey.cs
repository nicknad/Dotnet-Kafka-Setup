using System.Globalization;
using Confluent.Kafka;

namespace KafkaPipeline.Core.Messaging;

/// <summary>
/// Builds idempotency keys from a message's Kafka coordinates.
/// Keying by topic-partition-offset (rather than by payload such as
/// prime value + timestamp) is deliberate: the same prime value can
/// legitimately appear in many distinct messages, while a redelivery
/// after a crash or retry always reuses the same offset.
/// </summary>
public static class MessageIdempotencyKey
{
    public static string For(TopicPartitionOffset topicPartitionOffset) =>
        For(
            topicPartitionOffset.Topic,
            topicPartitionOffset.Partition.Value,
            topicPartitionOffset.Offset.Value);

    public static string For(string topic, int partition, long offset) =>
        string.Create(CultureInfo.InvariantCulture, $"{topic}-{partition}-{offset}");
}
