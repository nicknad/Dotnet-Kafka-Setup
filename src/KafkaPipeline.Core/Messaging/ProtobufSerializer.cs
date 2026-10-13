using Confluent.Kafka;
using Google.Protobuf;

namespace KafkaPipeline.Core.Messaging;

/// <summary>
/// Serializes protobuf messages. The payload is always pure protobuf — envelope framing
/// (schema version / content type) lives in Kafka record <see cref="Headers"/> via
/// <see cref="MessageEnvelope"/>, because <see cref="ISerializer{T}.Serialize"/>
/// has no access to headers. The produce site stamps them (see
/// <see cref="SerializeWithEnvelope"/> or <see cref="MessageEnvelope.ApplyTo"/>).
/// </summary>
public sealed class ProtobufSerializer<T> : ISerializer<T> where T : IMessage<T>
{
    public byte[] Serialize(T data, SerializationContext context) => data.ToByteArray();

    /// <summary>
    /// Serializes the payload and stamps the envelope headers on the same record's headers.
    /// Call this at the produce site before <c>ProduceAsync</c> so versioning stays in one place.
    /// </summary>
    public byte[] SerializeWithEnvelope(
        T data,
        SerializationContext context,
        Headers headers,
        ISchemaVersionProvider schemaVersions)
    {
        ArgumentNullException.ThrowIfNull(data);
        ArgumentNullException.ThrowIfNull(headers);
        ArgumentNullException.ThrowIfNull(schemaVersions);

        var messageName = typeof(T).Name;
        MessageEnvelope.ApplyTo(headers, schemaVersions.CurrentVersion, messageName);
        return Serialize(data, context);
    }
}
