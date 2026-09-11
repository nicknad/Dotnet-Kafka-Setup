using Confluent.Kafka;
using Google.Protobuf;

namespace KafkaPipeline.Core.Messaging;

public sealed class ProtobufSerializer<T> : ISerializer<T> where T : IMessage<T>
{
    public byte[] Serialize(T data, SerializationContext context) => data.ToByteArray();
}
