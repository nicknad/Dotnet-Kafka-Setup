using System.Diagnostics;
using Confluent.Kafka;
using KafkaPipeline.Core.Configuration;
using KafkaPipeline.Core.Messages;
using KafkaPipeline.Core.Messaging;
using KafkaPipeline.Core.Observability;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;

namespace KafkaConsumerApp;

internal sealed class ConsumerWorker(
    IOptions<KafkaOptions> options,
    ILogger<ConsumerWorker> logger) : BackgroundService
{
    protected override async Task ExecuteAsync(CancellationToken stoppingToken)
    {
        await Task.Yield();

        var kafka = options.Value;
        var config = new ConsumerConfig
        {
            BootstrapServers = kafka.BootstrapServers,
            GroupId = kafka.ConsumerGroup,
            AutoOffsetReset = AutoOffsetReset.Earliest,
            EnableAutoCommit = false
        };

        using var consumer = new ConsumerBuilder<Ignore, PrimeNumber>(config)
            .SetValueDeserializer(new ProtobufDeserializer<PrimeNumber>())
            .SetErrorHandler((_, error) => logger.LogError("Kafka error: {Reason} ({Code})", error.Reason, error.Code))
            .SetPartitionsAssignedHandler((_, partitions) => logger.LogInformation(
                "Assigned partitions: {Partitions}",
                string.Join(", ", partitions.Select(partition => partition.Partition.Value))))
            .SetPartitionsRevokedHandler((_, partitions) => logger.LogInformation(
                "Revoked partitions: {Partitions}",
                string.Join(", ", partitions.Select(partition => partition.Partition.Value))))
            .Build();

        consumer.Subscribe(kafka.Topic);
        logger.LogInformation("Subscribed to {Topic} as consumer group {GroupId}", kafka.Topic, config.GroupId);

        try
        {
            while (!stoppingToken.IsCancellationRequested)
            {
                ConsumeResult<Ignore, PrimeNumber>? result;
                try
                {
                    result = consumer.Consume(stoppingToken);
                }
                catch (OperationCanceledException)
                {
                    break;
                }

                if (result?.Message?.Value is null)
                {
                    if (result is not null)
                    {
                        logger.LogWarning("Skipping tombstone at {TopicPartitionOffset}", result.TopicPartitionOffset);
                    }

                    continue;
                }

                Process(result, kafka.Topic);
                consumer.Commit(result);
            }
        }
        catch (OperationCanceledException) when (stoppingToken.IsCancellationRequested)
        {
            logger.LogInformation("Shutdown requested");
        }
        finally
        {
            consumer.Close();
        }
    }

    private void Process(ConsumeResult<Ignore, PrimeNumber> result, string topic)
    {
        var parentContext = KafkaTraceContext.Extract(result.Message.Headers);
        using var activity = ActivitySources.Consumer.StartActivity("process prime", ActivityKind.Consumer, parentContext ?? default);

        activity?.SetTag("messaging.system", "kafka");
        activity?.SetTag("messaging.destination.name", topic);
        activity?.SetTag("messaging.kafka.partition", result.Partition.Value);
        activity?.SetTag("messaging.kafka.offset", result.Offset.Value);
        activity?.SetTag("prime.value", result.Message.Value.Value);

        logger.LogInformation(
            "Consumed prime {Prime} from {Topic} partition {Partition} at offset {Offset}",
            result.Message.Value.Value,
            topic,
            result.Partition.Value,
            result.Offset.Value);
    }
}
