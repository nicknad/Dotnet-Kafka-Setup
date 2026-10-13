using Confluent.Kafka;
using KafkaPipeline.Core.Messages;

namespace KafkaPipeline.Core.Messaging;

/// <summary>
/// Default <see cref="IPrimeProcessor"/>: admits a message for processing only
/// when its topic-partition-offset key has not been seen before. The
/// <paramref name="prime"/> payload is carried for future enrichment of the
/// dedup decision (and keeps the call site honest about what is being gated);
/// the key itself stays offset-based so identical payloads at different
/// offsets are never conflated.
/// </summary>
public sealed class PrimeProcessor(IIdempotencyStore store) : IPrimeProcessor
{
    private readonly IIdempotencyStore _store = store ?? throw new ArgumentNullException(nameof(store));

    public PrimeProcessOutcome Process(TopicPartitionOffset topicPartitionOffset, PrimeNumber prime)
    {
        ArgumentNullException.ThrowIfNull(prime);

        return _store.TryAdd(MessageIdempotencyKey.For(topicPartitionOffset))
            ? PrimeProcessOutcome.Processed
            : PrimeProcessOutcome.AlreadySeen;
    }
}
