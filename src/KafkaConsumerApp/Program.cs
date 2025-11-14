using Confluent.Kafka;
using KafkaConsumerApp;
using Messages;
using OpenTelemetry;
using OpenTelemetry.Trace;
using Serilog;

Log.Logger = new LoggerConfiguration()
    .MinimumLevel.Debug()
    .WriteTo.Console()
    .CreateLogger();

Log.Information("Starting Consumer...");
using var tracer_provider = Sdk.CreateTracerProviderBuilder()
    .AddSource("KafkaConsumer")
    .AddConsoleExporter()
    .Build();

var tracer = tracer_provider.GetTracer("KafkaConsumer");
var topic_name = "primes-topic";

var config = new ConsumerConfig
{
    BootstrapServers = "kafka:9092",
    GroupId = "prime-consumer-group",
    AutoOffsetReset = AutoOffsetReset.Earliest, 
    EnableAutoCommit = false
};

using (var c = new ConsumerBuilder<Ignore, PrimeNumber>(config)
    .SetValueDeserializer(new ProtobufDeserializer<PrimeNumber>())
    .SetErrorHandler((_, e) => Log.Error($"Kafka Error: {e.Reason}"))
    .SetPartitionsAssignedHandler((c, partitions) => Log.Information($"Assigned partitions: {string.Join(", ", partitions.Select(p => p.Partition.Value))}"))
    .SetPartitionsRevokedHandler((c, partitions) => Log.Information($"Revoked partitions: {string.Join(", ", partitions.Select(p => p.Partition.Value))}"))
    .Build())
{
    Log.Information($"Consumer connected, Group ID: {config.GroupId}");
    c.Subscribe(topic_name);

    using var activity = tracer.StartRootSpan("Consume");

    try
    {
        while (true)
        {
            var consume_result = c.Consume(TimeSpan.FromMilliseconds(100));

            if (consume_result != null)
            {
                var prime_message = consume_result.Message.Value;
                
                using var consume_activity = tracer.StartActiveSpan($"ProcessPrime-{prime_message.Value}", SpanKind.Internal, activity);
                consume_activity?.SetAttribute("kafka.topic", topic_name);
                consume_activity?.SetAttribute("prime.value", prime_message.Value);
                Log.Information($"Consumed Prime: {prime_message.Value} at Partition: {consume_result.Partition.Value}, Offset: {consume_result.Offset.Value}");
                c.Commit(consume_result);
            }
        }
    }
    catch (OperationCanceledException)
    {
    }
    finally
    {
        c.Close();
    }
}
Log.CloseAndFlush();