#if WINDOWS
using System.Text;
using Confluent.Kafka;
using Google.Protobuf;
using KafkaConsumerApp;
using KafkaPipeline.Core.Messages;
using KafkaPipeline.Core.Messaging;
using KafkaPipeline.Core.Hosting;
using KafkaPipeline.IntegrationTests.Support;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using Wslc.Testcontainers.Modules.Kafka;
using Xunit;

namespace KafkaPipeline.IntegrationTests;

/// <summary>
/// Broker-backed tests running on a real Kafka broker started through WSL Containers.
/// Enabled only in the GitHub Actions Windows job (WSLC_RUN_INTEGRATION=1).
/// </summary>
public sealed class KafkaPipelineIntegrationTests
{
    [WslcIntegrationFact]
    public async Task Produces_and_consumes_a_versioned_prime()
    {
        await using var kafka = new KafkaBuilder().Build();
        await kafka.StartAsync();
        var bootstrapServers = kafka.GetBootstrapServers();
        var topic = $"primes-roundtrip-{Guid.NewGuid():N}";

        using (var producer = new ProducerBuilder<Null, byte[]>(KafkaClients.IdempotentProducerConfig(bootstrapServers))
            .SetValueSerializer(Serializers.ByteArray)
            .Build())
        {
            var headers = new Headers();
            MessageEnvelope.ApplyTo(headers, SchemaVersions.CurrentVersion, nameof(PrimeNumber));
            await producer.ProduceAsync(topic, new Message<Null, byte[]>
            {
                Value = new PrimeNumber { Value = 997, Timestamp = 123 }.ToByteArray(),
                Headers = headers
            });
        }

        var result = ConsumeOne(bootstrapServers, topic, $"group-{Guid.NewGuid():N}", TimeSpan.FromSeconds(60));

        Assert.NotNull(result);
        Assert.True(
            MessageEnvelope.TryGetVersion(result!.Message.Headers, out var version, out var versionError),
            versionError);
        Assert.Equal(SchemaVersions.CurrentVersion, version);
        Assert.True(
            ProtobufMessageParser.TryParse<PrimeNumber>(result.Message.Value, result.Message.Headers, provider: null, out var prime, out var parseError),
            parseError);
        Assert.NotNull(prime);
        Assert.Equal(997, prime!.Value);
    }

    [WslcIntegrationFact]
    public async Task Consumer_worker_dead_letters_malformed_messages_and_commits_offsets()
    {
        await using var kafka = new KafkaBuilder().Build();
        await kafka.StartAsync();
        var bootstrapServers = kafka.GetBootstrapServers();
        var suffix = Guid.NewGuid().ToString("N");
        var topic = $"primes-{suffix}";
        var deadLetterTopic = $"{topic}.dlq";
        var group = $"group-{suffix}";
        var malformed = "not-a-protobuf"u8.ToArray();

        // Seed both messages before the worker starts; AutoOffsetReset.Earliest makes
        // consumption deterministic without racing the group join.
        await ProduceAsync(bootstrapServers, topic, new PrimeNumber { Value = 101, Timestamp = 1 }.ToByteArray());
        await ProduceAsync(bootstrapServers, topic, malformed);

        var builder = Host.CreateEmptyApplicationBuilder(new HostApplicationBuilderSettings());
        builder.Configuration.AddInMemoryCollection(new Dictionary<string, string?>
        {
            ["Kafka:BootstrapServers"] = bootstrapServers,
            ["Kafka:Topic"] = topic,
            ["Kafka:ConsumerGroup"] = group,
            ["Kafka:DeadLetterTopic"] = deadLetterTopic,
            ["Kafka:RetryTopic"] = $"{topic}.retry",
            ["Kafka:CommitBatchSize"] = "1",
            ["Kafka:MaxRetryAttempts"] = "0"
        });
        builder.Services.AddLogging();
        builder.AddKafkaOptions();
        builder.Services.AddSingleton<IIdempotencyStore>(new InMemoryIdempotencyStore(1_000));
        builder.Services.AddSingleton<IPrimeProcessor, PrimeProcessor>();
        builder.Services.AddStaticSchemaVersioning();
        builder.Services.AddHostedService<ConsumerWorker>();

        using var host = builder.Build();
        await host.StartAsync();
        try
        {
            var deadLettered = ConsumeOne(bootstrapServers, deadLetterTopic, $"dlq-{suffix}", TimeSpan.FromSeconds(90));

            Assert.NotNull(deadLettered);
            Assert.Equal(malformed, deadLettered!.Message.Value);
            Assert.Equal(
                topic,
                Encoding.UTF8.GetString(LastHeader(deadLettered.Message.Headers, DeadLetterHeaders.OriginalTopic)!));
            Assert.False(string.IsNullOrEmpty(
                Encoding.UTF8.GetString(LastHeader(deadLettered.Message.Headers, DeadLetterHeaders.ErrorReason)!)));

            // Both messages were handled (one processed, one dead-lettered) => commits advanced past both.
            Assert.True(WaitForCommittedOffset(bootstrapServers, group, topic, expectedOffset: 2, TimeSpan.FromSeconds(30)));
        }
        finally
        {
            await host.StopAsync();
        }
    }

    private static async Task ProduceAsync(string bootstrapServers, string topic, byte[] payload)
    {
        using var producer = new ProducerBuilder<Null, byte[]>(KafkaClients.IdempotentProducerConfig(bootstrapServers))
            .SetValueSerializer(Serializers.ByteArray)
            .Build();

        await producer.ProduceAsync(topic, new Message<Null, byte[]> { Value = payload });
    }

    private static ConsumeResult<Ignore, byte[]>? ConsumeOne(
        string bootstrapServers,
        string topic,
        string group,
        TimeSpan timeout)
    {
        using var consumer = new ConsumerBuilder<Ignore, byte[]>(new ConsumerConfig
        {
            BootstrapServers = bootstrapServers,
            GroupId = group,
            AutoOffsetReset = AutoOffsetReset.Earliest
        })
            .SetValueDeserializer(Deserializers.ByteArray)
            .Build();
        consumer.Subscribe(topic);

        var deadline = DateTime.UtcNow + timeout;
        while (DateTime.UtcNow < deadline)
        {
            var result = consumer.Consume(TimeSpan.FromSeconds(2));
            if (result is not null)
            {
                return result;
            }
        }

        return null;
    }

    private static bool WaitForCommittedOffset(
        string bootstrapServers,
        string group,
        string topic,
        long expectedOffset,
        TimeSpan timeout)
    {
        using var consumer = new ConsumerBuilder<Ignore, byte[]>(new ConsumerConfig
        {
            BootstrapServers = bootstrapServers,
            GroupId = group
        })
            .SetValueDeserializer(Deserializers.ByteArray)
            .Build();

        var deadline = DateTime.UtcNow + timeout;
        var partitions = new[] { new TopicPartition(topic, 0) };
        while (DateTime.UtcNow < deadline)
        {
            var committed = consumer.Committed(partitions, TimeSpan.FromSeconds(5));
            if (committed.Count > 0 && committed[0].Offset.Value >= expectedOffset)
            {
                return true;
            }

            Thread.Sleep(500);
        }

        return false;
    }

    private static byte[]? LastHeader(Headers headers, string key) =>
        headers.TryGetLastBytes(key, out var value) ? value : null;
}
#endif
