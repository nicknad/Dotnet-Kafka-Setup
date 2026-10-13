using System.Diagnostics;
using System.Globalization;
using System.Text;
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
    private const int CommitRetryCount = 3;
    private static readonly TimeSpan CommitRetryDelay = TimeSpan.FromMilliseconds(250);

    private readonly CommitTracker _commitTracker = new();

    protected override async Task ExecuteAsync(CancellationToken stoppingToken)
    {
        await Task.Yield();

        var kafka = options.Value;
        var consumerConfig = new ConsumerConfig
        {
            BootstrapServers = kafka.BootstrapServers,
            GroupId = kafka.ConsumerGroup,
            AutoOffsetReset = AutoOffsetReset.Earliest,
            EnableAutoCommit = false
        };

        var deadLetterTopic = string.IsNullOrWhiteSpace(kafka.DeadLetterTopic)
            ? $"{kafka.Topic}.dlq"
            : kafka.DeadLetterTopic;

        using var deadLetterProducer = new ProducerBuilder<Null, byte[]>(KafkaClients.IdempotentProducerConfig(kafka.BootstrapServers))
            .SetValueSerializer(Serializers.ByteArray)
            .SetErrorHandler((_, error) => logger.LogKafkaError(error))
            .Build();

        using var consumer = new ConsumerBuilder<Ignore, byte[]>(consumerConfig)
            .SetValueDeserializer(Deserializers.ByteArray)
            .SetErrorHandler((_, error) => logger.LogKafkaError(error))
            .SetPartitionsAssignedHandler((_, partitions) => logger.LogInformation(
                "Assigned partitions: {Partitions}",
                string.Join(", ", partitions.Select(partition => partition.Partition.Value))))
            .SetPartitionsRevokedHandler((c, partitions) =>
            {
                logger.LogInformation(
                    "Revoked partitions: {Partitions}",
                    string.Join(", ", partitions.Select(partition => partition.Partition.Value)));
                CommitProcessed(c, stoppingToken);
            })
            .Build();

        consumer.Subscribe(kafka.Topic);
        logger.LogInformation(
            "Subscribed to {Topic} as consumer group {GroupId}; malformed messages are dead-lettered to {DeadLetterTopic}",
            kafka.Topic,
            kafka.ConsumerGroup,
            deadLetterTopic);

        try
        {
            while (!stoppingToken.IsCancellationRequested)
            {
                ConsumeResult<Ignore, byte[]> result;
                try
                {
                    result = consumer.Consume(stoppingToken);
                }
                catch (OperationCanceledException)
                {
                    break;
                }
                catch (ConsumeException exception)
                {
                    if (exception.Error.IsFatal)
                    {
                        throw;
                    }

                    logger.LogError(exception, "Consume failed: {Reason} ({Code})", exception.Error.Reason, exception.Error.Code);
                    continue;
                }

                if (result.Message.Value is null)
                {
                    logger.LogWarning("Skipping tombstone at {TopicPartitionOffset}", result.TopicPartitionOffset);
                }
                else if (ProtobufMessageParser.TryParse<PrimeNumber>(result.Message.Value, out var prime, out var reason))
                {
                    Process(result, prime, kafka.Topic);
                }
                else
                {
                    await PublishToDeadLetterAsync(deadLetterProducer, deadLetterTopic, result, reason, stoppingToken);
                }

                _commitTracker.Track(result.TopicPartitionOffset);

                if (_commitTracker.ShouldCommit(kafka.CommitBatchSize, kafka.CommitInterval))
                {
                    await CommitProcessedAsync(consumer, stoppingToken);
                }
            }
        }
        catch (OperationCanceledException) when (stoppingToken.IsCancellationRequested)
        {
            logger.LogInformation("Shutdown requested");
        }
        finally
        {
            CommitProcessed(consumer, stoppingToken);
            deadLetterProducer.Flush(TimeSpan.FromSeconds(5));
            consumer.Close();
        }
    }

    private async Task PublishToDeadLetterAsync(
        IProducer<Null, byte[]> producer,
        string deadLetterTopic,
        ConsumeResult<Ignore, byte[]> result,
        string reason,
        CancellationToken cancellationToken)
    {
        using var activity = ActivitySources.Consumer.StartKafkaActivity(
            "dead-letter message", ActivityKind.Producer, result.Message.Headers, deadLetterTopic);

        activity?.SetTag("messaging.kafka.partition", result.Partition.Value);
        activity?.SetTag("messaging.kafka.offset", result.Offset.Value);

        var headers = new Headers();
        foreach (var header in result.Message.Headers)
        {
            headers.Add(header);
        }

        headers.Add(DeadLetterHeaders.OriginalTopic, Encoding.UTF8.GetBytes(result.Topic));
        headers.Add(DeadLetterHeaders.OriginalPartition, Encoding.UTF8.GetBytes(result.Partition.Value.ToString(CultureInfo.InvariantCulture)));
        headers.Add(DeadLetterHeaders.OriginalOffset, Encoding.UTF8.GetBytes(result.Offset.Value.ToString(CultureInfo.InvariantCulture)));
        headers.Add(DeadLetterHeaders.ErrorReason, Encoding.UTF8.GetBytes(reason));
        headers.Add(DeadLetterHeaders.ErrorTimestamp, Encoding.UTF8.GetBytes(DateTimeOffset.UtcNow.ToUnixTimeMilliseconds().ToString(CultureInfo.InvariantCulture)));

        var message = new Message<Null, byte[]>
        {
            Value = result.Message.Value,
            Headers = headers
        };

        try
        {
            await producer.ProduceAsync(deadLetterTopic, message, cancellationToken);
            logger.LogError(
                "Dead-lettered malformed message from {TopicPartitionOffset} to {DeadLetterTopic}: {Reason}",
                result.TopicPartitionOffset,
                deadLetterTopic,
                reason);
        }
        catch (ProduceException<Null, byte[]> exception)
        {
            activity?.SetStatus(ActivityStatusCode.Error, exception.Error.Reason);
            logger.LogError(
                exception,
                "Failed to dead-letter message from {TopicPartitionOffset}; stopping so the message is not skipped",
                result.TopicPartitionOffset);
            throw;
        }
    }

    private void Process(ConsumeResult<Ignore, byte[]> result, PrimeNumber prime, string topic)
    {
        using var activity = ActivitySources.Consumer.StartKafkaActivity(
            "process prime", ActivityKind.Consumer, result.Message.Headers, topic);

        activity?.SetTag("messaging.kafka.partition", result.Partition.Value);
        activity?.SetTag("messaging.kafka.offset", result.Offset.Value);
        activity?.SetTag("prime.value", prime.Value);

        logger.LogInformation(
            "Consumed prime {Prime} from {Topic} partition {Partition} at offset {Offset}",
            prime.Value,
            topic,
            result.Partition.Value,
            result.Offset.Value);
    }

    private async Task CommitProcessedAsync(IConsumer<Ignore, byte[]> consumer, CancellationToken cancellationToken)
    {
        if (!_commitTracker.HasPending)
        {
            return;
        }

        var offsets = _commitTracker.GetOffsetsToCommit();

        for (var attempt = 1; ; attempt++)
        {
            try
            {
                consumer.Commit(offsets);
                logger.LogDebug(
                    "Committed {Count} partition offset(s): {Offsets}",
                    offsets.Count,
                    string.Join(", ", offsets.Select(static offset => offset.ToString())));
                _commitTracker.Reset();
                return;
            }
            catch (KafkaException exception) when (IsRetriableCommitError(exception) && attempt < CommitRetryCount)
            {
                logger.LogWarning(
                    "Commit attempt {Attempt} of {Attempts} failed: {Reason}; retrying",
                    attempt,
                    CommitRetryCount,
                    exception.Error.Reason);
                try
                {
                    await Task.Delay(CommitRetryDelay, cancellationToken);
                }
                catch (OperationCanceledException)
                {
                    // Shutdown requested: leave offsets pending so the
                    // synchronous finally-commit can retry without delay.
                    return;
                }
            }
            catch (KafkaException exception)
            {
                logger.LogError(
                    exception,
                    "Failed to commit {Count} partition offset(s) ({Offsets}); those offsets will be reprocessed",
                    offsets.Count,
                    string.Join(", ", offsets.Select(static offset => offset.ToString())));
                _commitTracker.Reset();
                return;
            }
        }
    }

    private void CommitProcessed(IConsumer<Ignore, byte[]> consumer, CancellationToken cancellationToken = default)
    {
        if (!_commitTracker.HasPending)
        {
            return;
        }

        var offsets = _commitTracker.GetOffsetsToCommit();

        for (var attempt = 1; ; attempt++)
        {
            try
            {
                consumer.Commit(offsets);
                logger.LogDebug(
                    "Committed {Count} partition offset(s): {Offsets}",
                    offsets.Count,
                    string.Join(", ", offsets.Select(static offset => offset.ToString())));
                _commitTracker.Reset();
                return;
            }
            catch (KafkaException exception) when (IsRetriableCommitError(exception) && attempt < CommitRetryCount)
            {
                logger.LogWarning(
                    "Commit attempt {Attempt} of {Attempts} failed: {Reason}; retrying",
                    attempt,
                    CommitRetryCount,
                    exception.Error.Reason);
                // Cancellable delay (no Thread.Sleep): on shutdown the wait
                // completes immediately but the retry is still attempted.
                try
                {
                    Task.Delay(CommitRetryDelay, cancellationToken).GetAwaiter().GetResult();
                }
                catch (OperationCanceledException)
                {
                    // Fall through and retry immediately.
                }
            }
            catch (KafkaException exception)
            {
                logger.LogError(
                    exception,
                    "Failed to commit {Count} partition offset(s) ({Offsets}); those offsets will be reprocessed",
                    offsets.Count,
                    string.Join(", ", offsets.Select(static offset => offset.ToString())));
                _commitTracker.Reset();
                return;
            }
        }
    }

    private static bool IsRetriableCommitError(KafkaException exception) =>
        CommitTracker.IsRetriableCommitError(exception);
}
