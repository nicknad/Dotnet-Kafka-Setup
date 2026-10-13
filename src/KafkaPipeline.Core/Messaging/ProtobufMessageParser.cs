using System.Diagnostics.CodeAnalysis;
using Confluent.Kafka;
using Google.Protobuf;
using KafkaPipeline.Core.Messages;

namespace KafkaPipeline.Core.Messaging;

/// <summary>
/// Parses protobuf payloads with envelope version gating. Records whose
/// <c>schema.version</c> header is missing are treated as v1 (backwards compatibility
/// with pre-envelope payloads); records with an unsupported (unknown major/future)
/// version fail with a DLQ-suitable reason instead of attempting a parse.
/// </summary>
public static class ProtobufMessageParser
{
    private static class ParserCache<T> where T : IMessage<T>, new()
    {
        public static readonly MessageParser<T> Parser = new(() => new T());
    }

    public static bool TryParse<T>(
        ReadOnlySpan<byte> data,
        [NotNullWhen(true)] out T? message,
        [NotNullWhen(false)] out string? error) where T : class, IMessage<T>, new()
    {
        try
        {
            message = ParserCache<T>.Parser.ParseFrom(data);
            error = null;
            return true;
        }
        catch (InvalidProtocolBufferException exception)
        {
            message = null;
            error = exception.Message;
            return false;
        }
    }

    /// <summary>
    /// Version-gated parse using the static <see cref="SchemaVersions"/> policy.
    /// </summary>
    public static bool TryParse<T>(
        ReadOnlySpan<byte> data,
        Headers? headers,
        [NotNullWhen(true)] out T? message,
        [NotNullWhen(false)] out string? error) where T : class, IMessage<T>, new() =>
        TryParse<T>(data, headers, provider: null, out message, out error);

    /// <summary>
    /// Version-gated parse. The <paramref name="provider"/> decides the supported range
    /// (inject <see cref="ISchemaVersionProvider"/>); when <c>null</c>, the static
    /// <see cref="SchemaVersions"/> policy applies.
    /// </summary>
    public static bool TryParse<T>(
        ReadOnlySpan<byte> data,
        Headers? headers,
        ISchemaVersionProvider? provider,
        [NotNullWhen(true)] out T? message,
        [NotNullWhen(false)] out string? error) where T : class, IMessage<T>, new()
    {
        if (!MessageEnvelope.TryGetVersion(headers, out var version, out var versionError))
        {
            message = null;
            error = versionError ?? "Malformed schema.version header.";
            return false;
        }

        var supported = provider?.IsSupported(version) ?? SchemaVersions.IsSupported(version);
        if (!supported)
        {
            message = null;
            error = $"Unsupported schema version {version} for message {typeof(T).Name}; " +
                    $"this consumer supports versions {SchemaVersions.MinSupportedVersion}-{SchemaVersions.CurrentVersion}. " +
                    "Dead-letter for schema upgrade.";
            return false;
        }

        var parsed = TryParse(data, out message, out error);
        if (!parsed)
        {
            message = null;
            error ??= "Failed to parse protobuf payload.";
        }

        return parsed;
    }
}
