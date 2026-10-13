namespace KafkaPipeline.Core.Messaging;

/// <summary>
/// Thread-safe in-memory <see cref="IIdempotencyStore"/> with a hard bound
/// on memory growth. Insertion order is tracked in a FIFO queue; once the
/// entry count exceeds <see cref="MaxEntries"/>, the oldest keys are evicted.
/// FIFO eviction (rather than LRU) is sufficient here because offsets only
/// need to be remembered long enough to cover the redelivery window after a
/// crash, rebalance, or failed commit - redeliveries arrive for recent
/// offsets, not for offsets consumed arbitrarily far in the past.
/// </summary>
public sealed class InMemoryIdempotencyStore : IIdempotencyStore
{
    public const int DefaultMaxEntries = 100_000;

    private readonly int _maxEntries;
    private readonly System.Collections.Concurrent.ConcurrentDictionary<string, byte> _entries =
        new(StringComparer.Ordinal);
    private readonly System.Collections.Concurrent.ConcurrentQueue<string> _insertionOrder = new();
    private int _count;

    public InMemoryIdempotencyStore(int maxEntries = DefaultMaxEntries)
    {
        if (maxEntries < 1)
        {
            throw new ArgumentOutOfRangeException(nameof(maxEntries), "Must be at least 1.");
        }

        _maxEntries = maxEntries;
    }

    /// <summary>Maximum number of keys retained before oldest-first eviction.</summary>
    public int MaxEntries => _maxEntries;

    /// <summary>Approximate number of keys currently retained.</summary>
    public int Count => Volatile.Read(ref _count);

    public bool TryAdd(string key)
    {
        ArgumentException.ThrowIfNullOrEmpty(key);

        if (!_entries.TryAdd(key, 0))
        {
            return false;
        }

        _insertionOrder.Enqueue(key);
        if (Interlocked.Increment(ref _count) > _maxEntries)
        {
            EvictExcess();
        }

        return true;
    }

    public bool Contains(string key) => _entries.ContainsKey(key);

    private void EvictExcess()
    {
        while (Volatile.Read(ref _count) > _maxEntries)
        {
            if (!_insertionOrder.TryDequeue(out var oldest))
            {
                return;
            }

            if (_entries.TryRemove(oldest, out _))
            {
                Interlocked.Decrement(ref _count);
            }
        }
    }
}
