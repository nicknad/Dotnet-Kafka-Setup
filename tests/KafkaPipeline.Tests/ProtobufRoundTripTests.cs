using Confluent.Kafka;
using KafkaPipeline.Core.Messages;
using KafkaPipeline.Core.Messaging;
using Xunit;

namespace KafkaPipeline.Tests;

public sealed class ProtobufRoundTripTests
{
    private static readonly SerializationContext Context = new(MessageComponentType.Value, "primes-topic");

    private static PrimeNumber CreateMessage() => new()
    {
        Value = 7919,
        Timestamp = 1_700_000_000_000
    };

    private static byte[] Serialize(PrimeNumber message) =>
        new ProtobufSerializer<PrimeNumber>().Serialize(message, Context);

    [Fact]
    public void Serialize_ThenParse_RoundTripsMessage()
    {
        var original = CreateMessage();

        var bytes = Serialize(original);
        var parsed = ProtobufMessageParser.TryParse<PrimeNumber>(bytes, out var message, out var error);

        Assert.True(parsed);
        Assert.Null(error);
        Assert.Equal(original, message);
    }

    [Fact]
    public void Parse_GarbageBytes_FailsWithReason()
    {
        var parsed = ProtobufMessageParser.TryParse<PrimeNumber>([0xFF, 0xFF, 0xFF], out var message, out var error);

        Assert.False(parsed);
        Assert.Null(message);
        Assert.False(string.IsNullOrWhiteSpace(error));
    }

    [Fact]
    public void Parse_TruncatedMessage_Fails()
    {
        var bytes = Serialize(CreateMessage());

        var parsed = ProtobufMessageParser.TryParse<PrimeNumber>(bytes.AsSpan(0, bytes.Length - 1), out var message, out var error);

        Assert.False(parsed);
        Assert.Null(message);
        Assert.False(string.IsNullOrWhiteSpace(error));
    }
}
