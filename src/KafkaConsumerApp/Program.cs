using KafkaPipeline.Core.Configuration;
using KafkaPipeline.Core.Observability;
using KafkaConsumerApp;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using OpenTelemetry.Resources;
using OpenTelemetry.Trace;
using Serilog;

var builder = Host.CreateApplicationBuilder(new HostApplicationBuilderSettings
{
    Args = args,
    ContentRootPath = AppContext.BaseDirectory
});

builder.Services.AddSerilog((_, logger) => logger
    .MinimumLevel.Information()
    .WriteTo.Console());

builder.Services.AddOptions<KafkaOptions>()
    .Bind(builder.Configuration.GetSection(KafkaOptions.SectionName))
    .Validate(options => !string.IsNullOrWhiteSpace(options.BootstrapServers), "Kafka:BootstrapServers is required")
    .Validate(options => !string.IsNullOrWhiteSpace(options.Topic), "Kafka:Topic is required")
    .Validate(options => !string.IsNullOrWhiteSpace(options.ConsumerGroup), "Kafka:ConsumerGroup is required")
    .ValidateOnStart();

builder.Services.AddOpenTelemetry()
    .ConfigureResource(resource => resource.AddService("KafkaConsumerApp"))
    .WithTracing(tracing =>
    {
        tracing.AddSource(ActivitySources.Consumer.Name);

        if (Uri.TryCreate(builder.Configuration["Otlp:Endpoint"], UriKind.Absolute, out var endpoint))
        {
            tracing.AddOtlpExporter(exporter => exporter.Endpoint = endpoint);
        }
        else
        {
            tracing.AddConsoleExporter();
        }
    });

builder.Services.AddHostedService<ConsumerWorker>();

await builder.Build().RunAsync();
