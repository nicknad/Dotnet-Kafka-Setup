using Confluent.Kafka;
using KafkaPipeline.Core.Messaging;
using Xunit;

namespace KafkaPipeline.Tests;

public sealed class CommitTrackerTests
{
    private const string Topic = "primes-topic";

    [Fact]
    public void Track_SingleMessage_StoresNextOffset()
    {
        var tracker = new CommitTracker();
        tracker.Track(new TopicPartitionOffset(Topic, new Partition(0), new Offset(5)));

        var offsets = tracker.GetOffsetsToCommit();

        var single = Assert.Single(offsets);
        Assert.Equal(Topic, single.Topic);
        Assert.Equal(0, single.Partition.Value);
        // Commit(IEnumerable) commits exact offsets, so tracker must store consumed+1.
        Assert.Equal(6, single.Offset.Value);
        Assert.True(tracker.HasPending);
        Assert.Equal(1, tracker.HandledSinceCommit);
    }

    [Fact]
    public void Track_MultiplePartitions_KeepsEveryPartition()
    {
        // Regression test for the single-pendingCommit bug: committing only the
        // last ConsumeResult drops the other partitions' offsets.
        var tracker = new CommitTracker();
        tracker.Track(new TopicPartitionOffset(Topic, new Partition(0), new Offset(10)));
        tracker.Track(new TopicPartitionOffset(Topic, new Partition(1), new Offset(20)));
        tracker.Track(new TopicPartitionOffset(Topic, new Partition(2), new Offset(30)));

        var offsets = tracker.GetOffsetsToCommit();

        Assert.Equal(3, offsets.Count);
        Assert.Equal(11, offsets.Single(o => o.Partition.Value == 0).Offset.Value);
        Assert.Equal(21, offsets.Single(o => o.Partition.Value == 1).Offset.Value);
        Assert.Equal(31, offsets.Single(o => o.Partition.Value == 2).Offset.Value);
        Assert.Equal(3, tracker.HandledSinceCommit);
    }

    [Fact]
    public void Track_SamePartition_AdvancesToMaxOffset()
    {
        var tracker = new CommitTracker();
        tracker.Track(new TopicPartitionOffset(Topic, new Partition(0), new Offset(7)));
        tracker.Track(new TopicPartitionOffset(Topic, new Partition(0), new Offset(9)));
        // Stale/redelivered offset must not move the commit backwards.
        tracker.Track(new TopicPartitionOffset(Topic, new Partition(0), new Offset(8)));

        var offsets = tracker.GetOffsetsToCommit();

        var single = Assert.Single(offsets);
        Assert.Equal(10, single.Offset.Value);
        Assert.Equal(3, tracker.HandledSinceCommit);
    }

    [Fact]
    public void ShouldCommit_ByBatchSize_ReturnsTrue()
    {
        var tracker = new CommitTracker();
        tracker.Track(new TopicPartitionOffset(Topic, new Partition(0), new Offset(0)));
        tracker.Track(new TopicPartitionOffset(Topic, new Partition(1), new Offset(0)));

        Assert.True(tracker.ShouldCommit(batchSize: 2, interval: TimeSpan.FromHours(1)));
    }

    [Fact]
    public void ShouldCommit_BelowBatchSizeAndInterval_ReturnsFalse()
    {
        var tracker = new CommitTracker();
        tracker.Track(new TopicPartitionOffset(Topic, new Partition(0), new Offset(0)));

        Assert.False(tracker.ShouldCommit(batchSize: 100, interval: TimeSpan.FromHours(1)));
    }

    [Fact]
    public void ShouldCommit_NoPending_ReturnsFalse()
    {
        var tracker = new CommitTracker();

        Assert.False(tracker.ShouldCommit(batchSize: 1, interval: TimeSpan.Zero));
    }

    [Fact]
    public void ShouldCommit_IntervalElapsed_ReturnsTrue()
    {
        var tracker = new CommitTracker();
        tracker.Track(new TopicPartitionOffset(Topic, new Partition(0), new Offset(0)));

        // Zero interval is already elapsed, independent of batch size.
        Assert.True(tracker.ShouldCommit(batchSize: 100, interval: TimeSpan.Zero));
    }

    [Fact]
    public void Reset_ClearsOffsetsAndCounters()
    {
        var tracker = new CommitTracker();
        tracker.Track(new TopicPartitionOffset(Topic, new Partition(0), new Offset(1)));
        tracker.Track(new TopicPartitionOffset(Topic, new Partition(1), new Offset(2)));

        tracker.Reset();

        Assert.False(tracker.HasPending);
        Assert.Equal(0, tracker.HandledSinceCommit);
        Assert.Empty(tracker.GetOffsetsToCommit());
        Assert.False(tracker.ShouldCommit(batchSize: 1, interval: TimeSpan.Zero));
    }

    [Fact]
    public void IsRetriableCommitError_RebalanceInProgress_ReturnsTrue()
    {
        var exception = new KafkaException(new Error(ErrorCode.RebalanceInProgress, "rebalancing"));

        Assert.True(CommitTracker.IsRetriableCommitError(exception));
    }

    [Fact]
    public void IsRetriableCommitError_PermanentError_ReturnsFalse()
    {
        var exception = new KafkaException(new Error(ErrorCode.Unknown, "boom"));

        Assert.False(CommitTracker.IsRetriableCommitError(exception));
    }
}
