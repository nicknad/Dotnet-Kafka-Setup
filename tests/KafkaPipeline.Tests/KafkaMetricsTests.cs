using System.Diagnostics.Metrics;
using Confluent.Kafka;
using KafkaPipeline.Core.Messaging;
using KafkaPipeline.Core.Observability;
using Microsoft.Extensions.Logging;
using Xunit;

namespace KafkaPipeline.Tests;

public sealed class KafkaMetricsTests
{
    [Fact]
    public void Instruments_HaveExpectedNames()
    {
        Assert.Equal(KafkaMetrics.MeterName, KafkaMetrics.Meter.Name);
        Assert.Equal("messaging.produce.messages", KafkaMetrics.ProduceMessages.Name);
        Assert.Equal("messaging.consume.messages", KafkaMetrics.ConsumeMessages.Name);
        Assert.Equal("messaging.dlq.messages", KafkaMetrics.DeadLetterMessages.Name);
        Assert.Equal("messaging.produce.duration", KafkaMetrics.ProduceDuration.Name);
        Assert.Equal("messaging.consume.commit.duration", KafkaMetrics.CommitDuration.Name);
    }

    [Fact]
    public void RecordProduce_PublishesCounterAndHistogram()
    {
        var measurements = Collect(() => KafkaMetrics.RecordProduce("topic", 12.5, "ok"));

        Assert.Contains(measurements, m => m.Instrument == "messaging.produce.messages" && m.Value == 1);
        Assert.Contains(measurements, m => m.Instrument == "messaging.produce.duration" && m.Value == 12.5);
    }

    [Fact]
    public void RecordProduce_WithErrorCode_PublishesMeasurements()
    {
        var measurements = Collect(() => KafkaMetrics.RecordProduce("topic", 3.25, "error", "BrokerDown"));

        Assert.Contains(measurements, m => m.Instrument == "messaging.produce.messages" && m.Value == 1);
        Assert.Contains(measurements, m => m.Instrument == "messaging.produce.duration" && m.Value == 3.25);
    }

    [Theory]
    [InlineData("ok")]
    [InlineData("malformed")]
    [InlineData("tombstone")]
    public void RecordConsume_PublishesCounter(string status)
    {
        var measurements = Collect(() => KafkaMetrics.RecordConsume("topic", status));

        Assert.Contains(measurements, m => m.Instrument == "messaging.consume.messages" && m.Value == 1);
    }

    [Fact]
    public void RecordDeadLetter_PublishesCounter()
    {
        var measurements = Collect(() => KafkaMetrics.RecordDeadLetter("topic", "topic.dlq"));

        Assert.Contains(measurements, m => m.Instrument == "messaging.dlq.messages" && m.Value == 1);
    }

    [Theory]
    [InlineData("ok")]
    [InlineData("error")]
    public void RecordCommit_PublishesHistogram(string status)
    {
        var measurements = Collect(() => KafkaMetrics.RecordCommit(7.5, status));

        Assert.Contains(measurements, m => m.Instrument == "messaging.consume.commit.duration" && m.Value == 7.5);
    }

    [Fact]
    public void LogKafkaError_FatalError_LogsError()
    {
        var logger = new CapturingLogger<KafkaMetricsTests>();

        logger.LogKafkaError(new Error(ErrorCode.Local_Fatal, "fatal", true));

        Assert.Contains(logger.Entries, e => e.Level == LogLevel.Error);
    }

    [Fact]
    public void LogKafkaError_NonFatalBrokerError_LogsWarning()
    {
        var logger = new CapturingLogger<KafkaMetricsTests>();

        logger.LogKafkaError(new Error(ErrorCode.BrokerNotAvailable, "broker down"));

        Assert.DoesNotContain(logger.Entries, e => e.Level == LogLevel.Error);
        Assert.Contains(logger.Entries, e => e.Level == LogLevel.Warning);
    }

    [Fact]
    public void LogKafkaError_NonFatalLocalError_LogsWarning()
    {
        var logger = new CapturingLogger<KafkaMetricsTests>();

        logger.LogKafkaError(new Error(ErrorCode.Local_MsgTimedOut, "timed out"));

        Assert.DoesNotContain(logger.Entries, e => e.Level == LogLevel.Error);
        Assert.Contains(logger.Entries, e => e.Level == LogLevel.Warning);
    }

    [Fact]
    public void LogKafkaStatistics_ValidPayload_LogsLagSummary()
    {
        var logger = new CapturingLogger<KafkaMetricsTests>();
        const string json = """
            {
                "name": "test-client",
                "brokers": {
                    "1": { "state": "UP" },
                    "2": { "state": "DOWN" }
                },
                "topics": {
                    "primes": {
                        "partitions": {
                            "0": { "consumer_lag": 5 },
                            "1": { "consumer_lag": -1 },
                            "2": { "consumer_lag": 7 }
                        }
                    }
                }
            }
            """;

        logger.LogKafkaStatistics(json);

        Assert.Contains(logger.Entries, e => e.Level == LogLevel.Debug);
    }

    [Theory]
    [InlineData("not json at all")]
    [InlineData("")]
    [InlineData("   ")]
    [InlineData("""{"topics": "unexpected"}""")]
    public void LogKafkaStatistics_MalformedPayload_DoesNotThrow(string payload)
    {
        var logger = new CapturingLogger<KafkaMetricsTests>();

        var exception = Record.Exception(() => logger.LogKafkaStatistics(payload));

        Assert.Null(exception);
    }

    private static List<(string Instrument, double Value)> Collect(Action record)
    {
        var measurements = new List<(string Instrument, double Value)>();
        using var listener = new MeterListener();
        listener.InstrumentPublished = (instrument, listenerInstance) =>
        {
            if (instrument.Meter.Name == KafkaMetrics.MeterName)
            {
                listenerInstance.EnableMeasurementEvents(instrument);
            }
        };
        listener.SetMeasurementEventCallback<long>((instrument, value, _, _) =>
            measurements.Add((instrument.Name, value)));
        listener.SetMeasurementEventCallback<double>((instrument, value, _, _) =>
            measurements.Add((instrument.Name, value)));
        listener.Start();

        record();

        return measurements;
    }

    private sealed class CapturingLogger<T> : ILogger<T>
    {
        public List<(LogLevel Level, string Message)> Entries { get; } = new();

        public IDisposable? BeginScope<TState>(TState state) where TState : notnull => null;

        public bool IsEnabled(LogLevel logLevel) => true;

        public void Log<TState>(
            LogLevel logLevel,
            EventId eventId,
            TState state,
            Exception? exception,
            Func<TState, Exception?, string> formatter) =>
            Entries.Add((logLevel, formatter(state, exception)));
    }
}
