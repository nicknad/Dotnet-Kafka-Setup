using System.Diagnostics;
using Confluent.Kafka;
using KafkaPipeline.Core.Observability;
using Xunit;

namespace KafkaPipeline.Tests;

public sealed class KafkaTraceContextTests : IDisposable
{
    private readonly ActivityListener _listener;

    public KafkaTraceContextTests()
    {
        _listener = new ActivityListener
        {
            ShouldListenTo = source => source.Name == "KafkaPipeline.Producer",
            Sample = (ref ActivityCreationOptions<ActivityContext> _) => ActivitySamplingResult.AllDataAndRecorded
        };

        ActivitySource.AddActivityListener(_listener);
    }

    [Fact]
    public void Inject_ThenExtract_PreservesTraceAndSpanIds()
    {
        using var activity = ActivitySources.Producer.StartActivity("publish prime");
        Assert.NotNull(activity);

        var headers = new Headers();
        KafkaTraceContext.Inject(activity, headers);

        var extracted = KafkaTraceContext.Extract(headers);

        Assert.NotNull(extracted);
        Assert.Equal(activity!.TraceId, extracted!.Value.TraceId);
        Assert.Equal(activity.SpanId, extracted.Value.SpanId);
        Assert.True(extracted.Value.IsRemote);
    }

    [Fact]
    public void Extract_MissingHeader_ReturnsNull()
    {
        Assert.Null(KafkaTraceContext.Extract(new Headers()));
    }

    [Fact]
    public void Extract_MalformedHeader_ReturnsNull()
    {
        var headers = new Headers
        {
            { KafkaTraceContext.TraceParentHeader, "not-a-traceparent"u8.ToArray() }
        };

        Assert.Null(KafkaTraceContext.Extract(headers));
    }

    [Fact]
    public void Inject_NullActivity_DoesNotAddHeader()
    {
        var headers = new Headers();

        KafkaTraceContext.Inject(null, headers);

        Assert.Empty(headers);
    }

    public void Dispose() => _listener.Dispose();
}
