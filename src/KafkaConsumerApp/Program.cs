using KafkaPipeline.Core.Configuration;
using KafkaPipeline.Core.Hosting;
using KafkaPipeline.Core.Messaging;
using KafkaPipeline.Core.Observability;
using KafkaConsumerApp;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Options;

var builder = Host.CreateApplicationBuilder(new HostApplicationBuilderSettings
{
    Args = args,
    ContentRootPath = AppContext.BaseDirectory
});

builder
    .AddKafkaPipeline("KafkaConsumerApp", ActivitySources.Consumer)
    .AddKafkaOptions()
    .Validate(options => !string.IsNullOrWhiteSpace(options.ConsumerGroup), "Kafka:ConsumerGroup is required")
    .Validate(options => options.CommitBatchSize >= 1, "Kafka:CommitBatchSize must be at least 1")
    .Validate(options => options.CommitInterval > TimeSpan.Zero, "Kafka:CommitInterval must be positive")
    .Validate(options => options.IdempotencyMaxEntries >= 1, "Kafka:IdempotencyMaxEntries must be at least 1")
    .ValidateOnStart();

builder.Services.AddSingleton<IIdempotencyStore>(sp =>
    new InMemoryIdempotencyStore(sp.GetRequiredService<IOptions<KafkaOptions>>().Value.IdempotencyMaxEntries));
builder.Services.AddSingleton<IPrimeProcessor, PrimeProcessor>();
builder.Services.AddHostedService<ConsumerWorker>();

await builder.Build().RunAsync();
