using Confluent.Kafka;
using KafkaPipeline.Core.Messages;
using KafkaPipeline.Core.Messaging;
using Xunit;

namespace KafkaPipeline.Tests;

public sealed class ProtobufRoundTripTests
{
    private static readonly SerializationContext Context = new(MessageComponentType.Value, "primes-topic");

    [Fact]
    public void Serialize_ThenDeserialize_RoundTripsMessage()
    {
        var serializer = new ProtobufSerializer<PrimeNumber>();
        var deserializer = new ProtobufDeserializer<PrimeNumber>();
        var original = new PrimeNumber { Value = 7919, Timestamp = 1_700_000_000_000 };

        var bytes = serializer.Serialize(original, Context);
        var roundTripped = deserializer.Deserialize(bytes, isNull: false, Context);

        Assert.Equal(original, roundTripped);
    }

    [Fact]
    public void Deserialize_NullPayload_ReturnsNull()
    {
        var deserializer = new ProtobufDeserializer<PrimeNumber>();

        var result = deserializer.Deserialize(ReadOnlySpan<byte>.Empty, isNull: true, Context);

        Assert.Null(result);
    }
}
