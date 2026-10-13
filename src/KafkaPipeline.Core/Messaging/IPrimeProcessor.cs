using Confluent.Kafka;
using KafkaPipeline.Core.Messages;

namespace KafkaPipeline.Core.Messaging;

/// <summary>Outcome of passing a consumed message through <see cref="IPrimeProcessor"/>.</summary>
public enum PrimeProcessOutcome
{
    /// <summary>First time this message was seen; the caller should process it.</summary>
    Processed,

    /// <summary>Redelivery of an already-processed message; the caller must skip the work.</summary>
    AlreadySeen
}

/// <summary>
/// Idempotency gate for consumed prime messages. Checks the backing
/// <see cref="IIdempotencyStore"/> before processing so that duplicates
/// caused by at-least-once redelivery (crash before commit, retry, rebalance)
/// are reported as <see cref="PrimeProcessOutcome.AlreadySeen"/> instead of
/// being processed a second time.
/// </summary>
public interface IPrimeProcessor
{
    PrimeProcessOutcome Process(TopicPartitionOffset topicPartitionOffset, PrimeNumber prime);
}
