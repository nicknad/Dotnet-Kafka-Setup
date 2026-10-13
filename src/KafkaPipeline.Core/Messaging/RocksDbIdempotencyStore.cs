using System.Globalization;
using System.Text;
using RocksDbSharp;

namespace KafkaPipeline.Core.Messaging;

/// <summary>
/// Durable <see cref="IIdempotencyStore"/> backed by RocksDB, so processed
/// offsets survive process restarts — the redelivery window that matters most
/// for at-least-once consumption (crash after processing, before the offset
/// commit). Entries are bounded FIFO-style by <see cref="MaxEntries"/>: each
/// key is written together with a monotonically increasing sequence record in
/// a secondary index, and the oldest keys are trimmed once the bound is
/// exceeded.
/// </summary>
public sealed class RocksDbIdempotencyStore : IIdempotencyStore
{
    private const string KeyPrefix = "k:";
    private const string OrderPrefix = "q:";
    private const string NextSequenceKey = "m:next-sequence";
    private const string CountKey = "m:count";

    private static readonly byte[] OrderPrefixBytes = ToBytes(OrderPrefix);

    private readonly object _gate = new();
    private readonly RocksDb _db;
    private readonly int _maxEntries;
    private long _nextSequence;
    private long _count;
    private bool _disposed;

    public RocksDbIdempotencyStore(string path, int maxEntries = InMemoryIdempotencyStore.DefaultMaxEntries)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(path);
        if (maxEntries < 1)
        {
            throw new ArgumentOutOfRangeException(nameof(maxEntries), "Must be at least 1.");
        }

        _maxEntries = maxEntries;
        Directory.CreateDirectory(path);
        _db = RocksDb.Open(new DbOptions().SetCreateIfMissing(true), path);
        _nextSequence = ReadCounter(NextSequenceKey);
        _count = ReadCounter(CountKey);
    }

    /// <summary>Default on-disk location when no path is configured.</summary>
    public static string DefaultPath => Path.Combine(Path.GetTempPath(), "kafka-pipeline-idempotency");

    /// <summary>Maximum number of keys retained before oldest-first eviction.</summary>
    public int MaxEntries => _maxEntries;

    /// <inheritdoc />
    public bool TryAdd(string key)
    {
        ArgumentException.ThrowIfNullOrEmpty(key);
        ObjectDisposedException.ThrowIf(_disposed, this);

        lock (_gate)
        {
            var entryKey = ToBytes(KeyPrefix + key);
            if (_db.Get(entryKey) is not null)
            {
                return false;
            }

            var sequence = ++_nextSequence;
            _count++;
            using var batch = new WriteBatch();
            batch.Put(entryKey, Array.Empty<byte>());
            batch.Put(ToBytes(OrderPrefix + FormatSequence(sequence)), ToBytes(key));
            batch.Put(ToBytes(NextSequenceKey), ToBytes(FormatSequence(_nextSequence)));
            batch.Put(ToBytes(CountKey), ToBytes(FormatSequence(_count)));
            _db.Write(batch);

            TrimExcess();
            return true;
        }
    }

    /// <inheritdoc />
    public bool Contains(string key)
    {
        ArgumentException.ThrowIfNullOrEmpty(key);
        ObjectDisposedException.ThrowIf(_disposed, this);

        lock (_gate)
        {
            return _db.Get(ToBytes(KeyPrefix + key)) is not null;
        }
    }

    /// <inheritdoc />
    public void Dispose()
    {
        lock (_gate)
        {
            if (_disposed)
            {
                return;
            }

            _disposed = true;
            _db.Dispose();
        }
    }

    private void TrimExcess()
    {
        if (_count <= _maxEntries)
        {
            return;
        }

        using var batch = new WriteBatch();
        using var iterator = _db.NewIterator();
        iterator.Seek(OrderPrefixBytes);

        while (_count > _maxEntries && iterator.Valid())
        {
            var orderKey = iterator.Key();
            if (!StartsWith(orderKey, OrderPrefixBytes))
            {
                break;
            }

            batch.Delete(orderKey);
            batch.Delete(ToBytes(KeyPrefix + Encoding.UTF8.GetString(iterator.Value())));
            _count--;
            iterator.Next();
        }

        batch.Put(ToBytes(CountKey), ToBytes(FormatSequence(_count)));
        _db.Write(batch);
    }

    private long ReadCounter(string name)
    {
        var value = _db.Get(ToBytes(name));
        return value is not null
            && long.TryParse(Encoding.UTF8.GetString(value), NumberStyles.Integer, CultureInfo.InvariantCulture, out var count)
                ? count
                : 0;
    }

    private static string FormatSequence(long sequence) =>
        sequence.ToString("D20", CultureInfo.InvariantCulture);

    private static bool StartsWith(byte[] value, byte[] prefix) =>
        value.AsSpan().StartsWith(prefix);

    private static byte[] ToBytes(string value) => Encoding.UTF8.GetBytes(value);
}
