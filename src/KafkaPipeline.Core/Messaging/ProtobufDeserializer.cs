using Confluent.Kafka;
using Google.Protobuf;

namespace KafkaPipeline.Core.Messaging;

public sealed class ProtobufDeserializer<T> : IDeserializer<T> where T : IMessage<T>, new()
{
    private static readonly MessageParser<T> Parser = new(() => new T());

    public T Deserialize(ReadOnlySpan<byte> data, bool isNull, SerializationContext context)
    {
        if (isNull)
        {
            return default!;
        }

        return Parser.ParseFrom(data);
    }
}
