using Confluent.Kafka;
using Microsoft.Extensions.Logging;

namespace KafkaPipeline.Core.Messaging;

public static class KafkaLoggingExtensions
{
    public static void LogKafkaError(this ILogger logger, Error error) =>
        logger.LogError("Kafka error: {Reason} ({Code})", error.Reason, error.Code);
}
