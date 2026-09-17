using System.Diagnostics;
using KafkaPipeline.Core.Configuration;
using KafkaPipeline.Core.Observability;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Options;
using OpenTelemetry.Resources;
using OpenTelemetry.Trace;
using Serilog;

namespace KafkaPipeline.Core.Hosting;

public static class KafkaPipelineHostingExtensions
{
    public static IHostApplicationBuilder AddKafkaPipeline(
        this IHostApplicationBuilder builder,
        string serviceName,
        params ActivitySource[] sources)
    {
        builder.Services.AddSerilog((_, logger) => logger
            .MinimumLevel.Information()
            .WriteTo.Console());

        builder.Services.AddOpenTelemetry()
            .ConfigureResource(resource => resource.AddService(serviceName))
            .WithTracing(tracing =>
            {
                foreach (var source in sources)
                {
                    tracing.AddSource(source.Name);
                }

                if (Uri.TryCreate(builder.Configuration["Otlp:Endpoint"], UriKind.Absolute, out var endpoint))
                {
                    tracing.AddOtlpExporter(exporter => exporter.Endpoint = endpoint);
                }
                else
                {
                    tracing.AddConsoleExporter();
                }
            });

        return builder;
    }

    public static OptionsBuilder<KafkaOptions> AddKafkaOptions(this IHostApplicationBuilder builder) =>
        builder.Services.AddOptions<KafkaOptions>()
            .Bind(builder.Configuration.GetSection(KafkaOptions.SectionName))
            .Validate(options => !string.IsNullOrWhiteSpace(options.BootstrapServers), "Kafka:BootstrapServers is required")
            .Validate(options => !string.IsNullOrWhiteSpace(options.Topic), "Kafka:Topic is required");
}
