namespace KafkaPipeline.Core.Messaging;

/// <summary>
/// Version constants for the protobuf contracts shipped in this library.
/// Single place to bump when <c>prime.proto</c> changes incompatibly.
/// </summary>
public static class SchemaVersions
{
    /// <summary>Version stamped on every newly produced message.</summary>
    public const int CurrentVersion = 1;

    /// <summary>Oldest version this codebase can still read.</summary>
    public const int MinSupportedVersion = 1;

    /// <summary>
    /// Any version outside <c>[MinSupportedVersion, CurrentVersion]</c> is treated as an
    /// unknown (future/incompatible major) schema and routed to the DLQ by the consumer.
    /// </summary>
    public static bool IsSupported(int version) => version >= MinSupportedVersion && version <= CurrentVersion;
}
