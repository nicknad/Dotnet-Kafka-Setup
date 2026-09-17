using System.Diagnostics;
using Confluent.Kafka;

namespace KafkaPipeline.Core.Observability;

public static class KafkaActivityExtensions
{
    public static Activity? StartKafkaActivity(
        this ActivitySource source,
        string operationName,
        ActivityKind kind,
        Headers? parentHeaders,
        string destinationTopic)
    {
        var parent = KafkaTraceContext.Extract(parentHeaders);
        var activity = source.StartActivity(operationName, kind, parent ?? default);
        activity?.SetTag("messaging.system", "kafka");
        activity?.SetTag("messaging.destination.name", destinationTopic);
        return activity;
    }
}
