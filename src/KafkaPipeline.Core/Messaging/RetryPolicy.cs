using System.Globalization;
using System.Text;
using Confluent.Kafka;

namespace KafkaPipeline.Core.Messaging;

public enum RetryDecision
{
    Retry,
    DeadLetter
}

public static class RetryPolicy
{
    private static readonly TimeSpan MaxDelay = TimeSpan.FromSeconds(30);

    private const int MaxLastErrorBytes = 1024;

    public static string ResolveRetryTopic(string topic, string? configuredRetryTopic) =>
        string.IsNullOrWhiteSpace(configuredRetryTopic)
            ? $"{topic}.retry"
            : configuredRetryTopic;

    public static int GetAttemptCount(Headers? headers)
    {
        if (headers is null || !headers.TryGetLastBytes(RetryHeaders.Count, out var value) || value is null)
        {
            return 0;
        }

        if (!int.TryParse(Encoding.UTF8.GetString(value), NumberStyles.Integer, CultureInfo.InvariantCulture, out var count) || count < 0)
        {
            return 0;
        }

        return count;
    }

    public static long? GetFirstTimestampMs(Headers? headers)
    {
        if (headers is null || !headers.TryGetLastBytes(RetryHeaders.FirstTimestamp, out var value) || value is null)
        {
            return null;
        }

        if (!long.TryParse(Encoding.UTF8.GetString(value), NumberStyles.Integer, CultureInfo.InvariantCulture, out var timestamp) || timestamp < 0)
        {
            return null;
        }

        return timestamp;
    }

    public static string? GetLastError(Headers? headers)
    {
        if (headers is null || !headers.TryGetLastBytes(RetryHeaders.LastError, out var value) || value is null)
        {
            return null;
        }

        return Encoding.UTF8.GetString(value);
    }

    public static RetryDecision Decide(int attemptCount, int maxAttempts) =>
        attemptCount >= maxAttempts ? RetryDecision.DeadLetter : RetryDecision.Retry;

    public static RetryDecision Decide(Headers? headers, int maxAttempts) =>
        Decide(GetAttemptCount(headers), maxAttempts);

    public static TimeSpan GetRetryDelay(int nextAttemptNumber, int baseBackoffMs)
    {
        if (baseBackoffMs <= 0 || nextAttemptNumber <= 0)
        {
            return TimeSpan.Zero;
        }

        var multiplier = 1L << Math.Min(nextAttemptNumber - 1, 20);
        var delayMs = (long)baseBackoffMs * multiplier;
        var delay = TimeSpan.FromMilliseconds(Math.Min(delayMs, (long)MaxDelay.TotalMilliseconds));

        return delay < TimeSpan.Zero ? TimeSpan.Zero : delay;
    }

    public static Headers BuildRetryHeaders(Headers? original, string lastError, long? nowMs = null)
    {
        var nextCount = GetAttemptCount(original) + 1;
        var firstTimestamp = GetFirstTimestampMs(original) ?? nowMs ?? DateTimeOffset.UtcNow.ToUnixTimeMilliseconds();

        var headers = new Headers();
        if (original is not null)
        {
            foreach (var header in original)
            {
                if (string.Equals(header.Key, RetryHeaders.Count, StringComparison.Ordinal) ||
                    string.Equals(header.Key, RetryHeaders.FirstTimestamp, StringComparison.Ordinal) ||
                    string.Equals(header.Key, RetryHeaders.LastError, StringComparison.Ordinal))
                {
                    continue;
                }

                headers.Add(header);
            }
        }

        headers.Add(RetryHeaders.Count, Encoding.UTF8.GetBytes(nextCount.ToString(CultureInfo.InvariantCulture)));
        headers.Add(RetryHeaders.FirstTimestamp, Encoding.UTF8.GetBytes(firstTimestamp.ToString(CultureInfo.InvariantCulture)));
        headers.Add(RetryHeaders.LastError, Encoding.UTF8.GetBytes(Truncate(lastError)));

        return headers;
    }

    private static string Truncate(string value)
    {
        if (Encoding.UTF8.GetByteCount(value) <= MaxLastErrorBytes)
        {
            return value;
        }

        var bytes = Encoding.UTF8.GetBytes(value);
        var truncated = new byte[MaxLastErrorBytes];
        Buffer.BlockCopy(bytes, 0, truncated, 0, MaxLastErrorBytes);
        return Encoding.UTF8.GetString(truncated);
    }
}
