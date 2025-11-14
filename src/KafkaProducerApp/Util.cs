using Google.Protobuf;
using Confluent.Kafka;

namespace KafkaProducerApp;

internal static class Util
{
    extension(int num)
    {
        internal bool IsPrime() {
            if (num < 0) return false;
            if (num <= 3) return true;
            if (num % 2 == 0) return false;
            for (int i = 3; i * i <= num; i += 2)
            {
                if (num % i == 0) return false;
            }
                
            return true;
        }
    }
}

public class ProtobufSerializer<T> : ISerializer<T> where T : IMessage<T>, new()
{
    public byte[] Serialize(T data, SerializationContext context)
    {
        return data.ToByteArray();
    }
}
