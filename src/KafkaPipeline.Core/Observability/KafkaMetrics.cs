using System.Diagnostics;
using System.Diagnostics.Metrics;

namespace KafkaPipeline.Core.Observability;

/// <summary>
/// System.Diagnostics.Metrics instruments for the Kafka pipeline.
/// Meter name is <see cref="MeterName"/> ("KafkaPipeline").
/// </summary>
public static class KafkaMetrics
{
    public const string MeterName = "KafkaPipeline";

    public static readonly Meter Meter = new(MeterName);

    public static readonly Counter<long> ProduceMessages = Meter.CreateCounter<long>(
        "messaging.produce.messages",
        unit: "{message}",
        description: "Number of messages produced to Kafka.");

    public static readonly Counter<long> ConsumeMessages = Meter.CreateCounter<long>(
        "messaging.consume.messages",
        unit: "{message}",
        description: "Number of messages consumed from Kafka.");

    public static readonly Counter<long> DeadLetterMessages = Meter.CreateCounter<long>(
        "messaging.dlq.messages",
        unit: "{message}",
        description: "Number of messages routed to the dead-letter topic.");

    public static readonly Histogram<double> ProduceDuration = Meter.CreateHistogram<double>(
        "messaging.produce.duration",
        unit: "ms",
        description: "Duration of produce requests in milliseconds.");

    public static readonly Histogram<double> CommitDuration = Meter.CreateHistogram<double>(
        "messaging.consume.commit.duration",
        unit: "ms",
        description: "Duration of synchronous offset commits in milliseconds.");

    /// <summary>
    /// Records a produce attempt. Status is "ok" or "error"; code is the
    /// Kafka error code for failures and null for successes.
    /// </summary>
    public static void RecordProduce(string topic, double durationMs, string status, string? code = null)
    {
        var tags = code is null
            ? new TagList { { "topic", topic }, { "status", status } }
            : new TagList { { "topic", topic }, { "status", status }, { "code", code } };

        ProduceMessages.Add(1, tags);
        ProduceDuration.Record(durationMs, tags);
    }

    /// <summary>
    /// Records a consumed message. Status is "ok", "malformed" or "tombstone".
    /// </summary>
    public static void RecordConsume(string topic, string status) =>
        ConsumeMessages.Add(1, new TagList { { "topic", topic }, { "status", status } });

    /// <summary>
    /// Records a message successfully routed to the dead-letter topic.
    /// </summary>
    public static void RecordDeadLetter(string topic, string deadLetterTopic) =>
        DeadLetterMessages.Add(1, new TagList { { "topic", topic }, { "dead_letter_topic", deadLetterTopic } });

    /// <summary>
    /// Records the duration of a synchronous offset commit. Status is "ok" or "error".
    /// </summary>
    public static void RecordCommit(double durationMs, string status) =>
        CommitDuration.Record(durationMs, new TagList { { "status", status } });
}
