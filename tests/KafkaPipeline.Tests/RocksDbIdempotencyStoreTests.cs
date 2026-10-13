using KafkaPipeline.Core.Messaging;
using Xunit;

namespace KafkaPipeline.Tests;

public sealed class RocksDbIdempotencyStoreTests : IDisposable
{
    private readonly string _path = Path.Combine(
        Path.GetTempPath(),
        "kafka-pipeline-tests",
        Guid.NewGuid().ToString("N"));

    [Fact]
    public void TryAdd_NewKey_ReturnsTrue_AndContainsReportsIt()
    {
        using var store = new RocksDbIdempotencyStore(_path);

        Assert.True(store.TryAdd("primes-0-1"));
        Assert.True(store.Contains("primes-0-1"));
        Assert.False(store.Contains("primes-0-2"));
    }

    [Fact]
    public void TryAdd_DuplicateKey_ReturnsFalse()
    {
        using var store = new RocksDbIdempotencyStore(_path);

        Assert.True(store.TryAdd("primes-0-1"));
        Assert.False(store.TryAdd("primes-0-1"));
    }

    [Fact]
    public void Entries_SurviveReopen()
    {
        using (var store = new RocksDbIdempotencyStore(_path))
        {
            Assert.True(store.TryAdd("primes-0-1"));
        }

        using var reopened = new RocksDbIdempotencyStore(_path);

        Assert.True(reopened.Contains("primes-0-1"));
        Assert.False(reopened.TryAdd("primes-0-1"));
    }

    [Fact]
    public void TryAdd_EvictsOldestKeys_WhenBoundExceeded()
    {
        using var store = new RocksDbIdempotencyStore(_path, maxEntries: 3);

        store.TryAdd("k1");
        store.TryAdd("k2");
        store.TryAdd("k3");
        store.TryAdd("k4");

        Assert.False(store.Contains("k1"));
        Assert.True(store.Contains("k2"));
        Assert.True(store.Contains("k3"));
        Assert.True(store.Contains("k4"));
    }

    [Fact]
    public void EvictedKey_CanBeReAdded()
    {
        using var store = new RocksDbIdempotencyStore(_path, maxEntries: 1);

        Assert.True(store.TryAdd("k1"));
        Assert.True(store.TryAdd("k2"));

        Assert.True(store.TryAdd("k1"));
        Assert.True(store.Contains("k1"));
    }

    [Fact]
    public void Constructor_RejectsEmptyPath() =>
        Assert.Throws<ArgumentException>(() => new RocksDbIdempotencyStore("  "));

    [Theory]
    [InlineData(0)]
    [InlineData(-1)]
    public void Constructor_RejectsNonPositiveBound(int maxEntries) =>
        Assert.Throws<ArgumentOutOfRangeException>(() => new RocksDbIdempotencyStore(_path, maxEntries));

    public void Dispose()
    {
        try
        {
            Directory.Delete(_path, recursive: true);
        }
        catch (DirectoryNotFoundException)
        {
        }
    }
}
