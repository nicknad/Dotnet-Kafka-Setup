using KafkaPipeline.Core.Hosting;
using KafkaPipeline.Core.Observability;
using KafkaProducerApp;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;

var builder = Host.CreateApplicationBuilder(new HostApplicationBuilderSettings
{
    Args = args,
    ContentRootPath = AppContext.BaseDirectory
});

builder
    .AddKafkaPipeline("KafkaProducerApp", ActivitySources.Producer)
    .AddKafkaOptions()
    .ValidateOnStart();

builder.Services.AddHostedService<ProducerWorker>();

await builder.Build().RunAsync();
