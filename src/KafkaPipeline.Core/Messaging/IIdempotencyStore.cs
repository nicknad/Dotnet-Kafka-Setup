namespace KafkaPipeline.Core.Messaging;

/// <summary>
/// Bounded set of already-processed message keys used to make
/// at-least-once consumption idempotent. A redelivered message (same key)
/// must not be processed twice.
/// </summary>
public interface IIdempotencyStore
{
    /// <summary>
    /// Records <paramref name="key"/> as seen.
    /// Returns true when the key was first seen (caller should process),
    /// false when it was already present (duplicate, skip processing).
    /// </summary>
    bool TryAdd(string key);

    /// <summary>Returns true when <paramref name="key"/> was already seen.</summary>
    bool Contains(string key);
}
