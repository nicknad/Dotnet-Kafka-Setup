using Confluent.Kafka;
using Google.Protobuf;

namespace KafkaConsumerApp;

public class ProtobufDeserializer<T> : IDeserializer<T> where T : IMessage<T>, new()
{
    public T Deserialize(ReadOnlySpan<byte> data, bool isNull, SerializationContext context)
    {
        if (isNull) return default;
        var parser = new MessageParser<T>(() => new T());
        return parser.ParseFrom(data.ToArray());
    }
}