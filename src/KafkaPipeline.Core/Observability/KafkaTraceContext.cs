using System.Diagnostics;
using System.Globalization;
using System.Text;
using Confluent.Kafka;

namespace KafkaPipeline.Core.Observability;

public static class KafkaTraceContext
{
    public const string TraceParentHeader = "traceparent";

    public static void Inject(Activity? activity, Headers headers)
    {
        if (activity is null)
        {
            return;
        }

        var flags = activity.ActivityTraceFlags.HasFlag(ActivityTraceFlags.Recorded) ? "01" : "00";
        var traceParent = $"00-{activity.TraceId}-{activity.SpanId}-{flags}";

        headers.Remove(TraceParentHeader);
        headers.Add(TraceParentHeader, Encoding.UTF8.GetBytes(traceParent));
    }

    public static ActivityContext? Extract(Headers? headers)
    {
        if (headers is null || !headers.TryGetLastBytes(TraceParentHeader, out var value) || value is null)
        {
            return null;
        }

        var parts = Encoding.UTF8.GetString(value).Split('-');
        if (parts.Length != 4 || parts[0] != "00" || parts[1].Length != 32 || parts[2].Length != 16)
        {
            return null;
        }

        try
        {
            return new ActivityContext(
                ActivityTraceId.CreateFromString(parts[1]),
                ActivitySpanId.CreateFromString(parts[2]),
                (ActivityTraceFlags)byte.Parse(parts[3], NumberStyles.HexNumber, CultureInfo.InvariantCulture),
                traceState: null,
                isRemote: true);
        }
        catch (Exception exception) when (exception is ArgumentOutOfRangeException or FormatException)
        {
            return null;
        }
    }
}
