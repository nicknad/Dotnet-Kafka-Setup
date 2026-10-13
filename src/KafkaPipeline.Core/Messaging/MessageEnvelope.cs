using System.Globalization;
using System.Text;
using Confluent.Kafka;

namespace KafkaPipeline.Core.Messaging;

/// <summary>
/// Framing convention for protobuf messages on the wire.
///
/// Chosen approach: <b>header-based framing, payload stays pure protobuf</b>.
/// Each produced record carries two Kafka headers:
/// <list type="bullet">
///   <item><c>schema.version</c>: the schema version as UTF8-encoded int, e.g. <c>"1"</c>.</item>
///   <item><c>content.type</c>: <c>application/x-protobuf; message=&lt;ProtoMessageName&gt;</c>.</item>
/// </list>
///
/// Why headers instead of a magic-byte + version prefix in the payload:
/// <list type="bullet">
///   <item><c>ISerializer&lt;T&gt;.Serialize</c> has no access to record headers, but the
///   produce site does — stamping headers at message construction keeps the serializer
///   trivial and the payload decodable by any stock protobuf tooling.</item>
///   <item>No payload migration: existing raw-protobuf records (no headers) still parse —
///   a missing <c>schema.version</c> header is treated as v1 for backwards compatibility.</item>
///   <item>Trace headers (<c>traceparent</c>) and DLQ forwarding already operate on headers,
///   so versioning rides the same mechanism.</item>
/// </list>
/// A future move to Confluent Schema Registry (magic byte <c>0x00</c> + 4-byte BE schema id
/// prefix) would change the payload framing and is intentionally not mixed in here; the
/// <see cref="ISchemaVersionProvider"/> abstraction is the seam for that migration.
/// </summary>
public static class MessageEnvelope
{
    /// <summary>Kafka header carrying the schema version as UTF8-encoded int.</summary>
    public const string SchemaVersionHeader = "schema.version";

    /// <summary>Kafka header carrying the protobuf content type.</summary>
    public const string ContentTypeHeader = "content.type";

    /// <summary>MIME type for protobuf payloads.</summary>
    public const string ProtobufContentTypePrefix = "application/x-protobuf";

    /// <summary>Builds the <c>content.type</c> header value for a proto message name.</summary>
    public static string ContentTypeFor(string messageName) =>
        $"{ProtobufContentTypePrefix}; message={messageName}";

    /// <summary>
    /// Stamps the envelope headers onto a record being produced. Overwrites any stale
    /// <c>schema.version</c>/<c>content.type</c> entries so retries never double-stamp.
    /// </summary>
    public static void ApplyTo(Headers headers, int version, string messageName)
    {
        ArgumentNullException.ThrowIfNull(headers);
        ArgumentNullException.ThrowIfNull(messageName);

        headers.Remove(SchemaVersionHeader);
        headers.Remove(ContentTypeHeader);
        headers.Add(SchemaVersionHeader, Encoding.UTF8.GetBytes(version.ToString(CultureInfo.InvariantCulture)));
        headers.Add(ContentTypeHeader, Encoding.UTF8.GetBytes(ContentTypeFor(messageName)));
    }

    /// <summary>
    /// Reads the on-wire schema version. A missing header means "pre-envelope v1 payload"
    /// and returns <see cref="SchemaVersions.MinSupportedVersion"/> for backwards compatibility.
    /// </summary>
    /// <returns><c>false</c> with a DLQ-suitable reason when the header is present but malformed.</returns>
    public static bool TryGetVersion(Headers? headers, out int version, out string? error)
    {
        var raw = GetLastHeaderValue(headers, SchemaVersionHeader);
        if (raw is null)
        {
            version = SchemaVersions.MinSupportedVersion;
            error = null;
            return true;
        }

        var text = Encoding.UTF8.GetString(raw);
        if (int.TryParse(text, NumberStyles.Integer, CultureInfo.InvariantCulture, out version) && version >= 0)
        {
            error = null;
            return true;
        }

        version = 0;
        error = $"Malformed {SchemaVersionHeader} header value '{text}': expected a non-negative integer.";
        return false;
    }

    private static byte[]? GetLastHeaderValue(Headers? headers, string key)
    {
        if (headers is null)
        {
            return null;
        }

        byte[]? last = null;
        foreach (var header in headers)
        {
            if (string.Equals(header.Key, key, StringComparison.Ordinal))
            {
                last = header.GetValueBytes();
            }
        }

        return last;
    }
}
