namespace KafkaPipeline.Core.Messaging;

/// <summary>
/// Abstraction over "which schema version are we on / do we understand".
/// The default implementation is <see cref="StaticSchemaVersionProvider"/> (no external
/// service). Swapping to Confluent Schema Registry later is a one-line DI change:
/// replace <c>AddStaticSchemaVersioning()</c> with a registry-backed implementation
/// of this interface.
/// </summary>
public interface ISchemaVersionProvider
{
    /// <summary>Version to stamp on newly produced messages.</summary>
    int CurrentVersion { get; }

    /// <summary>Whether an observed on-wire version can be parsed by this codebase.</summary>
    bool IsSupported(int version);
}

/// <summary>
/// Compile-time schema versions, no network calls. Returns <see cref="SchemaVersions"/>.
/// </summary>
public sealed class StaticSchemaVersionProvider : ISchemaVersionProvider
{
    public int CurrentVersion => SchemaVersions.CurrentVersion;

    public bool IsSupported(int version) => SchemaVersions.IsSupported(version);
}
