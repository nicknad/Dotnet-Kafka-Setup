namespace KafkaPipeline.Core.Configuration;

/// <summary>Selects the <see cref="Messaging.IIdempotencyStore"/> implementation for the consumer.</summary>
public enum IdempotencyStoreKind
{
    /// <summary>Process-local store; deduplicates only within a single run (tests, short-lived tools).</summary>
    InMemory,

    /// <summary>Durable RocksDB store; survives restarts (default).</summary>
    RocksDb
}
