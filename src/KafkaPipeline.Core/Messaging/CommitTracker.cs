using System.Diagnostics;
using Confluent.Kafka;

namespace KafkaPipeline.Core.Messaging;

/// <summary>
/// Tracks the highest consumed offset per topic-partition so batched manual
/// commits cover every assigned partition (not just the last consumed message).
/// </summary>
/// <remarks>
/// <para>
/// Confluent's <c>Commit(ConsumeResult)</c> commits <c>Offset + 1</c> (next offset
/// to consume). <c>Commit(IEnumerable&lt;TopicPartitionOffset&gt;)</c> commits the
/// exact offsets passed, so <see cref="Track"/> stores <c>consumed + 1</c> to
/// preserve the single-message semantics across all partitions.
/// </para>
/// <para>Single-threaded by design: only the consumer poll thread touches it.</para>
/// </remarks>
public sealed class CommitTracker
{
    private readonly Dictionary<TopicPartition, Offset> _nextOffsets = new();
    private readonly Stopwatch _timer = Stopwatch.StartNew();
    private int _handledSinceCommit;

    public bool HasPending => _nextOffsets.Count > 0;

    public int HandledSinceCommit => _handledSinceCommit;

    /// <summary>
    /// Record a handled message (valid, tombstone, or DLQ'd) for later commit.
    /// </summary>
    /// <param name="consumed">The consumed position (raw offset, not +1).</param>
    public void Track(TopicPartitionOffset consumed)
    {
        if (consumed.Offset.IsSpecial)
        {
            return;
        }

        var next = new Offset(consumed.Offset.Value + 1);
        var key = new TopicPartition(consumed.Topic, consumed.Partition);

        if (_nextOffsets.TryGetValue(key, out var existing))
        {
            if (next.Value > existing.Value)
            {
                _nextOffsets[key] = next;
            }
        }
        else
        {
            _nextOffsets[key] = next;
        }

        _handledSinceCommit++;
    }

    public void Track(string topic, Partition partition, Offset offset) =>
        Track(new TopicPartitionOffset(topic, partition, offset));

    /// <summary>
    /// Snapshot of offsets to pass to <c>consumer.Commit(offsets)</c>.
    /// </summary>
    public IReadOnlyList<TopicPartitionOffset> GetOffsetsToCommit() =>
        _nextOffsets
            .Select(entry => new TopicPartitionOffset(entry.Key, entry.Value))
            .ToList();

    /// <summary>
    /// Batch by message count or elapsed time, matching ConsumerWorker semantics.
    /// </summary>
    public bool ShouldCommit(int batchSize, TimeSpan interval)
    {
        if (!HasPending)
        {
            return false;
        }

        return _handledSinceCommit >= batchSize || _timer.Elapsed >= interval;
    }

    public void Reset()
    {
        _nextOffsets.Clear();
        _handledSinceCommit = 0;
        _timer.Restart();
    }

    public static bool IsRetriableCommitError(KafkaException exception) =>
        exception is KafkaRetriableException || exception.Error.Code == ErrorCode.RebalanceInProgress;
}
