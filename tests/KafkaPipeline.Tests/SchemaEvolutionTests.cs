using System.Text;
using Confluent.Kafka;
using KafkaPipeline.Core.Messages;
using KafkaPipeline.Core.Messaging;
using Xunit;

namespace KafkaPipeline.Tests;

public sealed class SchemaEvolutionTests
{
    private static readonly SerializationContext Context = new(MessageComponentType.Value, "primes-topic");
    private static readonly ISchemaVersionProvider Schemas = new StaticSchemaVersionProvider();

    private static PrimeNumber CreateMessage() => new()
    {
        Value = 7919,
        Timestamp = 1_700_000_000_000
    };

    private static byte[] Serialize(PrimeNumber message) =>
        new ProtobufSerializer<PrimeNumber>().Serialize(message, Context);

    private static Headers HeadersWithVersion(int version, string messageName = "PrimeNumber")
    {
        var headers = new Headers();
        MessageEnvelope.ApplyTo(headers, version, messageName);
        return headers;
    }

    private static string? HeaderValue(Headers headers, string key)
    {
        foreach (var header in headers)
        {
            if (header.Key == key)
            {
                return Encoding.UTF8.GetString(header.GetValueBytes());
            }
        }

        return null;
    }

    [Fact]
    public void Produce_StampsVersionAndContentTypeHeaders()
    {
        var headers = HeadersWithVersion(Schemas.CurrentVersion);

        Assert.Equal(
            Schemas.CurrentVersion.ToString(),
            HeaderValue(headers, MessageEnvelope.SchemaVersionHeader));
        Assert.Equal(
            $"application/x-protobuf; message=PrimeNumber",
            HeaderValue(headers, MessageEnvelope.ContentTypeHeader));
    }

    [Fact]
    public void RoundTrip_WithCurrentVersionHeader_Parses()
    {
        var original = CreateMessage();
        var bytes = Serialize(original);
        var headers = HeadersWithVersion(SchemaVersions.CurrentVersion);

        var parsed = ProtobufMessageParser.TryParse<PrimeNumber>(bytes, headers, Schemas, out var message, out var error);

        Assert.True(parsed);
        Assert.Null(error);
        Assert.Equal(original, message);
    }

    [Fact]
    public void Parse_UnsupportedFutureVersion_RejectsForDeadLetter()
    {
        var bytes = Serialize(CreateMessage());
        var headers = HeadersWithVersion(SchemaVersions.CurrentVersion + 1);

        var parsed = ProtobufMessageParser.TryParse<PrimeNumber>(bytes, headers, Schemas, out var message, out var error);

        Assert.False(parsed);
        Assert.Null(message);
        Assert.Contains("Unsupported schema version", error, StringComparison.Ordinal);
    }

    [Fact]
    public void Parse_MissingVersionHeader_ParsesAsV1BackwardsCompat()
    {
        var original = CreateMessage();
        var bytes = Serialize(original);

        Assert.True(ProtobufMessageParser.TryParse<PrimeNumber>(bytes, headers: (Headers?)null, Schemas, out var viaNull, out _));
        Assert.Equal(original, viaNull);

        Assert.True(ProtobufMessageParser.TryParse<PrimeNumber>(bytes, new Headers(), Schemas, out var viaEmpty, out _));
        Assert.Equal(original, viaEmpty);
    }

    [Fact]
    public void Parse_MalformedVersionHeader_FailsWithReason()
    {
        var bytes = Serialize(CreateMessage());
        var headers = new Headers
        {
            { MessageEnvelope.SchemaVersionHeader, Encoding.UTF8.GetBytes("not-a-version") }
        };

        var parsed = ProtobufMessageParser.TryParse<PrimeNumber>(bytes, headers, Schemas, out var message, out var error);

        Assert.False(parsed);
        Assert.Null(message);
        Assert.False(string.IsNullOrWhiteSpace(error));
    }

    [Fact]
    public void SerializeWithEnvelope_StampsHeadersAndPayloadStaysParsable()
    {
        var original = CreateMessage();
        var headers = new Headers();
        var serializer = new ProtobufSerializer<PrimeNumber>();

        var bytes = serializer.SerializeWithEnvelope(original, Context, headers, Schemas);

        Assert.Equal("1", HeaderValue(headers, MessageEnvelope.SchemaVersionHeader));
        Assert.True(ProtobufMessageParser.TryParse<PrimeNumber>(bytes, headers, Schemas, out var message, out _));
        Assert.Equal(original, message);
    }

    [Fact]
    public void StaticProvider_ReturnsCurrentVersionAndRejectsFuture()
    {
        Assert.Equal(SchemaVersions.CurrentVersion, Schemas.CurrentVersion);
        Assert.True(Schemas.IsSupported(SchemaVersions.CurrentVersion));
        Assert.False(Schemas.IsSupported(SchemaVersions.CurrentVersion + 1));
    }
}
