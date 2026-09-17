using System.Diagnostics.CodeAnalysis;
using Google.Protobuf;
using KafkaPipeline.Core.Messages;

namespace KafkaPipeline.Core.Messaging;

public static class ProtobufMessageParser
{
    private static class ParserCache<T> where T : IMessage<T>, new()
    {
        public static readonly MessageParser<T> Parser = new(() => new T());
    }

    public static bool TryParse<T>(
        ReadOnlySpan<byte> data,
        [NotNullWhen(true)] out T? message,
        [NotNullWhen(false)] out string? error) where T : class, IMessage<T>, new()
    {
        try
        {
            message = ParserCache<T>.Parser.ParseFrom(data);
            error = null;
            return true;
        }
        catch (InvalidProtocolBufferException exception)
        {
            message = null;
            error = exception.Message;
            return false;
        }
    }
}
