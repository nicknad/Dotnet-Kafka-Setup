using Confluent.Kafka;
using KafkaPipeline.Core.Messages;
using KafkaPipeline.Core.Messaging;
using Xunit;

namespace KafkaPipeline.Tests;

public sealed class IdempotencyTests
{
    private static PrimeNumber Prime(int value = 7919) => new()
    {
        Value = value,
        Timestamp = 1_700_000_000_000
    };

    private static TopicPartitionOffset Tpo(string topic = "primes", int partition = 0, long offset = 1) =>
        new(topic, partition, offset);

    private static IPrimeProcessor Processor(int maxEntries = 100_000) =>
        new PrimeProcessor(new InMemoryIdempotencyStore(maxEntries));

    [Fact]
    public void KeyFormat_IsTopicPartitionOffset()
    {
        Assert.Equal("primes-1-42", MessageIdempotencyKey.For(Tpo("primes", 1, 42)));
    }

    [Fact]
    public void SameOffsetTwice_SecondIsDuplicate()
    {
        var processor = Processor();
        var tpo = Tpo(offset: 7);

        Assert.Equal(PrimeProcessOutcome.Processed, processor.Process(tpo, Prime()));
        Assert.Equal(PrimeProcessOutcome.AlreadySeen, processor.Process(tpo, Prime()));
    }

    [Fact]
    public void DifferentOffsets_BothProcessed()
    {
        var processor = Processor();

        Assert.Equal(PrimeProcessOutcome.Processed, processor.Process(Tpo(offset: 1), Prime()));
        Assert.Equal(PrimeProcessOutcome.Processed, processor.Process(Tpo(offset: 2), Prime()));
    }

    [Fact]
    public void SamePayloadAtDifferentOffsets_BothProcessed()
    {
        // TPO-keyed dedup must not conflate identical values in distinct messages.
        var processor = Processor();

        Assert.Equal(PrimeProcessOutcome.Processed, processor.Process(Tpo(offset: 1), Prime(7)));
        Assert.Equal(PrimeProcessOutcome.Processed, processor.Process(Tpo(offset: 2), Prime(7)));
    }

    [Fact]
    public void SameOffsetDifferentPartitions_AreDistinct()
    {
        var processor = Processor();

        Assert.Equal(PrimeProcessOutcome.Processed, processor.Process(Tpo(partition: 0, offset: 5), Prime()));
        Assert.Equal(PrimeProcessOutcome.Processed, processor.Process(Tpo(partition: 1, offset: 5), Prime()));
    }

    [Fact]
    public void Store_ContainsReflectsTryAdd()
    {
        var store = new InMemoryIdempotencyStore();

        Assert.False(store.Contains("primes-0-1"));
        Assert.True(store.TryAdd("primes-0-1"));
        Assert.True(store.Contains("primes-0-1"));
        Assert.False(store.TryAdd("primes-0-1"));
    }

    [Fact]
    public void Store_EvictsOldestWhenBoundExceeded()
    {
        var store = new InMemoryIdempotencyStore(maxEntries: 3);

        Assert.True(store.TryAdd("k1"));
        Assert.True(store.TryAdd("k2"));
        Assert.True(store.TryAdd("k3"));
        Assert.True(store.TryAdd("k4"));

        Assert.True(store.Count <= 3);
        Assert.False(store.Contains("k1"));
        Assert.True(store.Contains("k2"));
        Assert.True(store.Contains("k3"));
        Assert.True(store.Contains("k4"));
    }

    [Fact]
    public void Store_EvictedKeyCanBeSeenAgain()
    {
        var store = new InMemoryIdempotencyStore(maxEntries: 1);

        Assert.True(store.TryAdd("k1"));
        Assert.True(store.TryAdd("k2")); // evicts k1
        Assert.False(store.Contains("k1"));

        // Bounded memory means very old redeliveries may reprocess; the bound
        // must cover the redelivery window, which this behavior documents.
        Assert.True(store.TryAdd("k1"));
    }

    [Fact]
    public void Store_DuplicateAddDoesNotGrow()
    {
        var store = new InMemoryIdempotencyStore(maxEntries: 10);

        Assert.True(store.TryAdd("k1"));
        Assert.False(store.TryAdd("k1"));
        Assert.False(store.TryAdd("k1"));

        Assert.Equal(1, store.Count);
    }

    [Theory]
    [InlineData(0)]
    [InlineData(-1)]
    public void Store_RejectsNonPositiveBound(int maxEntries)
    {
        Assert.Throws<ArgumentOutOfRangeException>(() => new InMemoryIdempotencyStore(maxEntries));
    }

    [Fact]
    public void Processor_RejectsNullStore()
    {
        Assert.Throws<ArgumentNullException>(() => new PrimeProcessor(null!));
    }
}
