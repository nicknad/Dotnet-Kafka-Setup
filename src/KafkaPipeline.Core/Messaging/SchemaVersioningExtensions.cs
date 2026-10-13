using Microsoft.Extensions.DependencyInjection;

namespace KafkaPipeline.Core.Messaging;

/// <summary>
/// DI wiring for static (no external service) schema versioning.
/// To migrate to Confluent Schema Registry later, replace this call with a registration
/// of a registry-backed <see cref="ISchemaVersionProvider"/> — no worker code changes needed.
/// </summary>
public static class SchemaVersioningExtensions
{
    public static IServiceCollection AddStaticSchemaVersioning(this IServiceCollection services) =>
        services.AddSingleton<ISchemaVersionProvider, StaticSchemaVersionProvider>();
}
