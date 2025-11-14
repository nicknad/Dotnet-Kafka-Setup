using Confluent.Kafka;
using KafkaProducerApp;
using Messages;
using OpenTelemetry;
using OpenTelemetry.Trace;
using Serilog;

Log.Logger = new LoggerConfiguration()
    .MinimumLevel.Debug()
    .WriteTo.Console()
    .CreateLogger();

Log.Information("Starting Producer...");
using var tracer_provider = Sdk.CreateTracerProviderBuilder()
    .AddSource("KafkaProducer")
    .AddConsoleExporter()
    .Build();

var tracer = tracer_provider.GetTracer("KafkaProducer");
var topic_name = "primes-topic";
var config = new ProducerConfig
{
    BootstrapServers = "kafka:9092",
    Acks = Acks.All, 
    EnableIdempotence = true 
};
using (var p = new ProducerBuilder<Null, PrimeNumber>(config)
    .SetValueSerializer(new ProtobufSerializer<PrimeNumber>())
    .SetErrorHandler((_, e) => Log.Error($"Kafka Error: {e.Reason}"))
    .Build())
{
    Log.Information($"Producer connected to broker at {config.BootstrapServers}");
    
    using var activity = tracer.StartRootSpan("ProducePrimes");
    try
    {
        for (int i = 1; i < 10_000; i++)
        {
            if (i.IsPrime())
            {
                var prime_msg = new PrimeNumber
                {
                    Value = i,
                    Timestamp = DateTimeOffset.UtcNow.ToUnixTimeMilliseconds(),
                };

                using var prime_activity = tracer.StartActiveSpan($"ProducePrime-{i}", SpanKind.Internal, activity);
                prime_activity?.SetAttribute("kafka.topic", topic_name);
                prime_activity?.SetAttribute("prime.value", i);
                var delivery_result = await p.ProduceAsync(topic_name, new Message<Null, PrimeNumber> { Value = prime_msg });

                Log.Debug($"Published Prime: {i} to Partition: {delivery_result.Partition.Value}, Offset: {delivery_result.Offset.Value}");
            }
            Task.Delay(10).Wait();
        }
    }
    catch (ProduceException<Null, PrimeNumber> e)
    {
        Log.Fatal($"Delivery failed: {e.Error.Reason}");
    }
}
Log.CloseAndFlush();