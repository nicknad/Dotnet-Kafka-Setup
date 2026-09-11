using System.Diagnostics;
using Confluent.Kafka;
using KafkaPipeline.Core;
using KafkaPipeline.Core.Configuration;
using KafkaPipeline.Core.Messages;
using KafkaPipeline.Core.Messaging;
using KafkaPipeline.Core.Observability;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;

namespace KafkaProducerApp;

internal sealed class ProducerWorker(
    IOptions<KafkaOptions> options,
    ILogger<ProducerWorker> logger) : BackgroundService
{
    private const int MaxCandidate = 10_000;
    private const int PublishIntervalMs = 10;

    protected override async Task ExecuteAsync(CancellationToken stoppingToken)
    {
        var kafka = options.Value;
        var config = new ProducerConfig
        {
            BootstrapServers = kafka.BootstrapServers,
            Acks = Acks.All,
            EnableIdempotence = true
        };

        using var producer = new ProducerBuilder<Null, PrimeNumber>(config)
            .SetValueSerializer(new ProtobufSerializer<PrimeNumber>())
            .SetErrorHandler((_, error) => logger.LogError("Kafka error: {Reason} ({Code})", error.Reason, error.Code))
            .Build();

        logger.LogInformation("Publishing primes to {Topic} via {BootstrapServers}", kafka.Topic, kafka.BootstrapServers);

        try
        {
            for (var candidate = 2; candidate < MaxCandidate; candidate++)
            {
                if (candidate.IsPrime())
                {
                    await PublishPrimeAsync(producer, kafka.Topic, candidate, stoppingToken);
                }

                await Task.Delay(PublishIntervalMs, stoppingToken);
            }

            logger.LogInformation("Published all primes below {MaxCandidate}", MaxCandidate);
        }
        catch (OperationCanceledException) when (stoppingToken.IsCancellationRequested)
        {
            logger.LogInformation("Shutdown requested");
        }
        finally
        {
            producer.Flush(TimeSpan.FromSeconds(5));
        }
    }

    private async Task PublishPrimeAsync(
        IProducer<Null, PrimeNumber> producer,
        string topic,
        int value,
        CancellationToken cancellationToken)
    {
        using var activity = ActivitySources.Producer.StartActivity("publish prime", ActivityKind.Producer);
        activity?.SetTag("messaging.system", "kafka");
        activity?.SetTag("messaging.destination.name", topic);
        activity?.SetTag("prime.value", value);

        var headers = new Headers();
        KafkaTraceContext.Inject(activity, headers);

        var message = new Message<Null, PrimeNumber>
        {
            Value = new PrimeNumber
            {
                Value = value,
                Timestamp = DateTimeOffset.UtcNow.ToUnixTimeMilliseconds()
            },
            Headers = headers
        };

        try
        {
            var delivery = await producer.ProduceAsync(topic, message, cancellationToken);
            activity?.SetTag("messaging.kafka.partition", delivery.Partition.Value);
            activity?.SetTag("messaging.kafka.offset", delivery.Offset.Value);
            logger.LogDebug(
                "Published prime {Prime} to partition {Partition} at offset {Offset}",
                value,
                delivery.Partition.Value,
                delivery.Offset.Value);
        }
        catch (ProduceException<Null, PrimeNumber> exception)
        {
            activity?.SetStatus(ActivityStatusCode.Error, exception.Error.Reason);
            logger.LogError(exception, "Failed to publish prime {Prime}: {Reason}", value, exception.Error.Reason);

            if (exception.Error.IsFatal)
            {
                throw;
            }
        }
    }
}
