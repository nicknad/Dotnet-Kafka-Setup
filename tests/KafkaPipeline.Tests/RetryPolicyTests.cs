using System.Text;
using Confluent.Kafka;
using KafkaPipeline.Core.Messaging;
using Xunit;

namespace KafkaPipeline.Tests;

public sealed class RetryPolicyTests
{
    [Fact]
    public void GetAttemptCount_MissingHeaders_ReturnsZero()
    {
        Assert.Equal(0, RetryPolicy.GetAttemptCount(new Headers()));
        Assert.Equal(0, RetryPolicy.GetAttemptCount(null));
    }

    [Fact]
    public void GetAttemptCount_ValidHeader_ParsesValue()
    {
        var headers = new Headers { { RetryHeaders.Count, Encoding.UTF8.GetBytes("2") } };

        Assert.Equal(2, RetryPolicy.GetAttemptCount(headers));
    }

    [Theory]
    [InlineData("not-a-number")]
    [InlineData("")]
    [InlineData("-1")]
    public void GetAttemptCount_InvalidHeader_ReturnsZero(string raw)
    {
        var headers = new Headers { { RetryHeaders.Count, Encoding.UTF8.GetBytes(raw) } };

        Assert.Equal(0, RetryPolicy.GetAttemptCount(headers));
    }

    [Theory]
    [InlineData(0, 3, RetryDecision.Retry)]
    [InlineData(2, 3, RetryDecision.Retry)]
    [InlineData(3, 3, RetryDecision.DeadLetter)]
    [InlineData(5, 3, RetryDecision.DeadLetter)]
    [InlineData(0, 0, RetryDecision.DeadLetter)]
    public void Decide_MapsAttemptCountToOutcome(int attempts, int max, RetryDecision expected)
    {
        Assert.Equal(expected, RetryPolicy.Decide(attempts, max));
    }

    [Fact]
    public void Decide_FromHeaders_UsesCount()
    {
        var headers = new Headers { { RetryHeaders.Count, Encoding.UTF8.GetBytes("3") } };

        Assert.Equal(RetryDecision.DeadLetter, RetryPolicy.Decide(headers, 3));
    }

    [Theory]
    [InlineData(1, 1000, 1000)]
    [InlineData(2, 1000, 2000)]
    [InlineData(3, 1000, 4000)]
    public void GetRetryDelay_IsExponential(int attempt, int baseMs, int expectedMs)
    {
        Assert.Equal(TimeSpan.FromMilliseconds(expectedMs), RetryPolicy.GetRetryDelay(attempt, baseMs));
    }

    [Fact]
    public void GetRetryDelay_NonPositiveBase_ReturnsZero()
    {
        Assert.Equal(TimeSpan.Zero, RetryPolicy.GetRetryDelay(1, 0));
        Assert.Equal(TimeSpan.Zero, RetryPolicy.GetRetryDelay(1, -5));
    }

    [Fact]
    public void BuildRetryHeaders_FirstAttempt_SetsCountOneAndTimestamp()
    {
        var headers = RetryPolicy.BuildRetryHeaders(new Headers(), "boom", nowMs: 12345);

        Assert.Equal(1, RetryPolicy.GetAttemptCount(headers));
        Assert.Equal(12345, RetryPolicy.GetFirstTimestampMs(headers));
        Assert.Equal("boom", RetryPolicy.GetLastError(headers));
    }

    [Fact]
    public void BuildRetryHeaders_PreservesFirstTimestampAndIncrementsCount()
    {
        var original = new Headers
        {
            { RetryHeaders.Count, Encoding.UTF8.GetBytes("1") },
            { RetryHeaders.FirstTimestamp, Encoding.UTF8.GetBytes("999") },
            { RetryHeaders.LastError, Encoding.UTF8.GetBytes("first") }
        };

        var headers = RetryPolicy.BuildRetryHeaders(original, "second", nowMs: 55555);

        Assert.Equal(2, RetryPolicy.GetAttemptCount(headers));
        Assert.Equal(999, RetryPolicy.GetFirstTimestampMs(headers));
        Assert.Equal("second", RetryPolicy.GetLastError(headers));
    }

    [Fact]
    public void BuildRetryHeaders_PreservesNonRetryHeadersWithoutDuplicatingRetryOnes()
    {
        var original = new Headers
        {
            { "traceparent", Encoding.UTF8.GetBytes("00-abc-def-01") },
            { RetryHeaders.Count, Encoding.UTF8.GetBytes("1") }
        };

        var headers = RetryPolicy.BuildRetryHeaders(original, "err", nowMs: 1);

        var countHeaders = 0;
        string? traceparent = null;
        foreach (var header in headers)
        {
            if (header.Key == RetryHeaders.Count)
            {
                countHeaders++;
            }

            if (header.Key == "traceparent")
            {
                traceparent = Encoding.UTF8.GetString(header.GetValueBytes());
            }
        }

        Assert.Equal(1, countHeaders);
        Assert.Equal("00-abc-def-01", traceparent);
    }

    [Theory]
    [InlineData("primes-topic", null, "primes-topic.retry")]
    [InlineData("primes-topic", "", "primes-topic.retry")]
    [InlineData("primes-topic", "custom.retry", "custom.retry")]
    public void ResolveRetryTopic_FallsBackToConvention(string topic, string? configured, string expected)
    {
        Assert.Equal(expected, RetryPolicy.ResolveRetryTopic(topic, configured));
    }
}
