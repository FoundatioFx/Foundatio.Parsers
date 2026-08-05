using System;
using System.Collections.Concurrent;
using System.Threading;
using Elastic.Clients.Elasticsearch.Mapping;
using Microsoft.Extensions.Logging;

namespace Foundatio.Parsers.ElasticQueries;

/// <summary>
/// Owns the loaded server mapping and everything derived from it: when to reload, how reloads are coalesced,
/// and the per-field resolutions cached against each loaded mapping.
/// </summary>
/// <remarks>
/// Split out of <see cref="ElasticMappingResolver"/> so that refresh policy can be reasoned about (and
/// tested) independently of field name resolution. The resolver asks two questions — "what is the current
/// mapping?" and "the field was not in it, is there a newer one?" — and this type answers both.
/// </remarks>
internal sealed class MappingCache : IDisposable
{
    private readonly Func<TypeMapping?>? _getServerMapping;
    private readonly Func<TypeMapping?, Properties?> _mergeProperties;
    private readonly TimeProvider _timeProvider;
    private readonly ILogger _logger;

    private readonly ConcurrentDictionary<string, FieldMapping> _fields = new(StringComparer.Ordinal);
    private readonly SemaphoreSlim _refreshSemaphore = new(1, 1);
    private readonly object _publishLock = new();

    private MappingSnapshot _snapshot;
    private long _snapshotVersion;
    private long _resetVersion;
    private long _cachedFieldCount;
    private long _lastRefreshTimestamp;
    private long _lastUnmappedRefreshTimestamp;
    private long _unmappedRefreshBackoffTicks;
    private long _backoffAppliedForVersion;
    private long _lastSuppressedRefreshWarningTimestamp;
    private volatile bool _disposed;

    public MappingCache(Func<TypeMapping?>? getServerMapping, Func<TypeMapping?, Properties?> mergeProperties, TimeProvider timeProvider, ILogger logger)
    {
        _getServerMapping = getServerMapping;
        _mergeProperties = mergeProperties;
        _timeProvider = timeProvider;
        _logger = logger;
        _snapshot = CreateSnapshot(null, fetched: false);
    }

    public TimeSpan RefreshInterval { get; set; } = TimeSpan.FromMinutes(1);

    public TimeSpan UnmappedFieldRefreshInterval { get; set; } = TimeSpan.FromSeconds(5);

    public TimeSpan RefreshWaitTimeout { get; set; } = TimeSpan.FromSeconds(30);

    public int MaxCachedFields { get; set; } = 10000;

    public bool HasServerMappingFunc => _getServerMapping is not null;

    public long CachedFieldCount => Interlocked.Read(ref _cachedFieldCount);

    /// <summary>The currently published mapping. Never null; read without locking.</summary>
    public MappingSnapshot Current => Volatile.Read(ref _snapshot);

    /// <summary>
    /// Discards the loaded mapping and every field resolved from it, and clears all refresh throttles so the
    /// next resolution fetches immediately.
    /// </summary>
    public void Reset()
    {
        lock (_publishLock)
        {
            Interlocked.Increment(ref _resetVersion);
            ClearFields();
            Volatile.Write(ref _snapshot, CreateSnapshot(null, fetched: false));
            Interlocked.Exchange(ref _lastRefreshTimestamp, 0);
            Interlocked.Exchange(ref _lastUnmappedRefreshTimestamp, 0);
            Interlocked.Exchange(ref _unmappedRefreshBackoffTicks, 0);
        }
    }

    /// <summary>
    /// Drops the cached resolution for a single field and clears the unmapped field throttle so the next
    /// resolution of any unmapped field can refresh immediately.
    /// </summary>
    public void InvalidateField(string field)
    {
        if (_fields.TryRemove(field, out _))
            Interlocked.Decrement(ref _cachedFieldCount);

        Interlocked.Exchange(ref _lastUnmappedRefreshTimestamp, 0);
        Interlocked.Exchange(ref _unmappedRefreshBackoffTicks, 0);
    }

    public bool TryGetField(string field, MappingSnapshot snapshot, out FieldMapping mapping)
    {
        return _fields.TryGetValue(field, out mapping!) && mapping.Epoch == snapshot.Version;
    }

    /// <summary>
    /// Caches a field resolution, dropping it if the mapping it was resolved against has since been replaced.
    /// </summary>
    public void CacheField(string field, FieldMapping mapping, long snapshotVersion)
    {
        int maxCachedFields = MaxCachedFields;
        if (maxCachedFields <= 0)
            return;

        // Do not publish a resolution that was made against a mapping which has already been replaced.
        if (Current.Version != snapshotVersion)
            return;

        if (Interlocked.Read(ref _cachedFieldCount) >= maxCachedFields && !_fields.ContainsKey(field) && !TryMakeRoom(maxCachedFields))
            return;

        if (_fields.TryAdd(field, mapping))
        {
            Interlocked.Increment(ref _cachedFieldCount);
            return;
        }

        _fields.AddOrUpdate(field, mapping, (_, existing) => existing.Epoch > mapping.Epoch ? existing : mapping);
    }

    /// <summary>
    /// Evicts cached misses to make room for a new entry. Field names come from user supplied queries, so
    /// the unbounded growth vector is names that do not exist in the mapping; resolved fields are bounded by
    /// the size of the mapping itself. Evicting misses first therefore sheds exactly the entries an abusive
    /// caller can create without discarding the resolutions real queries depend on.
    /// </summary>
    /// <returns>True if there is now room for another entry.</returns>
    private bool TryMakeRoom(int maxCachedFields)
    {
        int removed = 0;
        foreach (var entry in _fields)
        {
            if (entry.Value.Found)
                continue;

            if (_fields.TryRemove(entry.Key, out _))
            {
                Interlocked.Decrement(ref _cachedFieldCount);
                removed++;
            }
        }

        if (removed > 0)
        {
            _logger.LogDebug("Evicted {RemovedCount} unresolved field names from the mapping cache after reaching {MaxCachedFields} entries", removed, maxCachedFields);
            return true;
        }

        // Every entry is a resolved field, so the mapping genuinely has more fields than the cache allows.
        // Stop adding rather than clearing: the entries already cached are the useful ones, and fields past
        // the bound still resolve correctly, just without caching.
        if (ShouldLogSuppressedRefresh())
            _logger.LogWarning("Mapping cache is full at {MaxCachedFields} resolved fields, so further fields will be resolved without caching. Increase {Property} if the index mapping has more fields than this", maxCachedFields, nameof(MaxCachedFields));

        return false;
    }

    /// <summary>
    /// Reloads the server mapping and publishes a new snapshot when it succeeds. Only one refresh runs at a
    /// time; callers that arrive while one is in flight wait for it rather than issuing their own.
    /// </summary>
    /// <param name="triggeredByUnmappedField">
    /// True when the refresh was triggered by a field that could not be resolved. Those refreshes use their
    /// own much shorter, self backing off throttle so a cold start fetch cannot suppress them.
    /// </param>
    public MappingRefreshResult Refresh(bool triggeredByUnmappedField)
    {
        var getServerMapping = _getServerMapping;
        if (getServerMapping is null || _disposed)
            return MappingRefreshResult.Skipped;

        if (!IsRefreshAllowed(triggeredByUnmappedField))
            return MappingRefreshResult.Throttled;

        long versionBeforeWait = Current.Version;

        bool acquired;
        try
        {
            // Wait for an in-flight refresh instead of silently continuing with a stale mapping. That refresh
            // is exactly the work this resolution needs, so waiting for it is never more expensive than doing
            // it here. The wait is bounded because the fetch callback is user supplied and does blocking
            // network I/O, and an unresponsive cluster must not pin request threads forever.
            acquired = _refreshSemaphore.Wait(GetRefreshWaitTimeout());
        }
        catch (ObjectDisposedException)
        {
            return MappingRefreshResult.Skipped;
        }

        if (!acquired)
        {
            // The in-flight refresh may have published while we were giving up.
            var snapshotAfterTimeout = Current;
            if (snapshotAfterTimeout.Version != versionBeforeWait && snapshotAfterTimeout.Fetched)
                return MappingRefreshResult.Updated;

            return MappingRefreshResult.WaitTimedOut;
        }

        try
        {
            // Another caller finished a refresh while we waited, so adopt its result rather than refetching.
            // A snapshot that has not been fetched means Reset() ran while we waited, in which case we still
            // have to do the fetch ourselves.
            var snapshotAfterWait = Current;
            if (snapshotAfterWait.Version != versionBeforeWait && snapshotAfterWait.Fetched)
                return MappingRefreshResult.Updated;

            if (!IsRefreshAllowed(triggeredByUnmappedField))
                return MappingRefreshResult.Throttled;

            long resetVersion = Interlocked.Read(ref _resetVersion);

            TypeMapping? newMapping;
            try
            {
                newMapping = getServerMapping();
            }
            catch (Exception ex) when (ex is not OutOfMemoryException and not StackOverflowException)
            {
                // Record the attempt so a failing cluster is not retried for every unresolved field.
                RecordRefreshAttempt(triggeredByUnmappedField);
                if (triggeredByUnmappedField)
                    IncreaseUnmappedRefreshBackoff();

                _logger.LogError(ex, "Error getting server mapping: {Message}", ex.Message);
                return MappingRefreshResult.Failed;
            }

            var current = Current;
            if (newMapping is null && current.Fetched && !current.HasServerMapping)
            {
                // Nothing changed, so keep the current snapshot (and its resolved field cache) intact.
                RecordRefreshAttempt(triggeredByUnmappedField);
                if (triggeredByUnmappedField)
                    IncreaseUnmappedRefreshBackoff();

                return MappingRefreshResult.Failed;
            }

            lock (_publishLock)
            {
                // A Reset() during the fetch means this result may predate the schema change the caller knows
                // about, so discard it and let the next resolution fetch again.
                if (Interlocked.Read(ref _resetVersion) != resetVersion)
                    return MappingRefreshResult.Failed;

                // Clear before publishing so resolutions made against the new snapshot are not discarded.
                ClearFields();
                Volatile.Write(ref _snapshot, CreateSnapshot(newMapping, fetched: true));
                RecordRefreshAttempt(triggeredByUnmappedField);
            }

            _logger.LogInformation("Got server mapping");

            return MappingRefreshResult.Updated;
        }
        finally
        {
            try
            {
                _refreshSemaphore.Release();
            }
            catch (ObjectDisposedException)
            {
                // the cache was disposed while the refresh was in flight
            }
        }
    }

    /// <summary>
    /// Records that a field was still unresolvable after the mapping was refreshed, which means it probably
    /// does not exist at all. Backs off at most once per refresh: many concurrent lookups adopt the result of
    /// a single refresh, and without that guard one refresh would ratchet the interval to the ceiling.
    /// </summary>
    public void RecordUnresolvedAfterRefresh(long snapshotVersion)
    {
        long applied = Interlocked.Read(ref _backoffAppliedForVersion);
        if (applied == snapshotVersion || Interlocked.CompareExchange(ref _backoffAppliedForVersion, snapshotVersion, applied) != applied)
            return;

        IncreaseUnmappedRefreshBackoff();
    }

    public void ResetUnmappedRefreshBackoff()
    {
        Interlocked.Exchange(ref _unmappedRefreshBackoffTicks, 0);
    }

    /// <summary>The interval currently in force for unmapped field refreshes, including any backoff.</summary>
    public TimeSpan CurrentUnmappedRefreshInterval => new(GetUnmappedRefreshIntervalTicks());

    /// <summary>
    /// Rate limits warnings about suppressed refreshes so a flood of queries against non-existent fields
    /// cannot flood the log. At most one warning is emitted per <see cref="RefreshInterval"/>.
    /// </summary>
    public bool ShouldLogSuppressedRefresh()
    {
        if (!_logger.IsEnabled(LogLevel.Warning))
            return false;

        long last = Interlocked.Read(ref _lastSuppressedRefreshWarningTimestamp);
        if (last != 0 && _timeProvider.GetElapsedTime(last) < RefreshInterval)
            return false;

        return Interlocked.CompareExchange(ref _lastSuppressedRefreshWarningTimestamp, _timeProvider.GetTimestamp(), last) == last;
    }

    private TimeSpan GetRefreshWaitTimeout()
    {
        var timeout = RefreshWaitTimeout;
        if (timeout == Timeout.InfiniteTimeSpan)
            return timeout;

        // SemaphoreSlim rejects waits longer than Int32.MaxValue milliseconds.
        return timeout.TotalMilliseconds > Int32.MaxValue ? TimeSpan.FromMilliseconds(Int32.MaxValue) : timeout;
    }

    private MappingSnapshot CreateSnapshot(TypeMapping? serverMapping, bool fetched)
    {
        long version = Interlocked.Increment(ref _snapshotVersion);

        // Properties are merged lazily and exactly once per snapshot: merging walks (and mutates) the whole
        // property tree, which is far too expensive to repeat for every field resolution.
        return new MappingSnapshot(version, serverMapping is not null, fetched,
            () => _mergeProperties(serverMapping), _timeProvider.GetUtcNow().UtcDateTime);
    }

    private long GetUnmappedRefreshIntervalTicks()
    {
        long baseTicks = Math.Max(UnmappedFieldRefreshInterval.Ticks, 0);
        long maxTicks = Math.Max(RefreshInterval.Ticks, baseTicks);
        long backoffTicks = Interlocked.Read(ref _unmappedRefreshBackoffTicks);

        return Math.Min(Math.Max(baseTicks, backoffTicks), maxTicks);
    }

    private void IncreaseUnmappedRefreshBackoff()
    {
        long baseTicks = Math.Max(UnmappedFieldRefreshInterval.Ticks, 0);
        long maxTicks = Math.Max(RefreshInterval.Ticks, baseTicks);
        long current = Interlocked.Read(ref _unmappedRefreshBackoffTicks);
        if (current >= maxTicks)
            return;

        long next = Math.Min(Math.Max(current, baseTicks) * 2, maxTicks);
        Interlocked.Exchange(ref _unmappedRefreshBackoffTicks, next);
    }

    private bool IsRefreshAllowed(bool triggeredByUnmappedField)
    {
        if (triggeredByUnmappedField)
        {
            long lastUnmappedRefresh = Interlocked.Read(ref _lastUnmappedRefreshTimestamp);
            return lastUnmappedRefresh == 0 || _timeProvider.GetElapsedTime(lastUnmappedRefresh) >= CurrentUnmappedRefreshInterval;
        }

        long lastRefresh = Interlocked.Read(ref _lastRefreshTimestamp);
        return lastRefresh == 0 || _timeProvider.GetElapsedTime(lastRefresh) >= RefreshInterval;
    }

    private void RecordRefreshAttempt(bool triggeredByUnmappedField)
    {
        long timestamp = _timeProvider.GetTimestamp();
        Interlocked.Exchange(ref _lastRefreshTimestamp, timestamp);
        if (triggeredByUnmappedField)
            Interlocked.Exchange(ref _lastUnmappedRefreshTimestamp, timestamp);
    }

    private void ClearFields()
    {
        _fields.Clear();
        Interlocked.Exchange(ref _cachedFieldCount, 0);
    }

    public void Dispose()
    {
        if (_disposed)
            return;

        _disposed = true;
        _refreshSemaphore.Dispose();
    }
}

/// <summary>
/// An immutable point-in-time view of the mapping. Published by reference swap so that readers never need a
/// lock and always observe a consistent mapping plus version pair.
/// </summary>
internal sealed class MappingSnapshot
{
    private readonly Lazy<Properties?> _properties;

    public MappingSnapshot(long version, bool hasServerMapping, bool fetched, Func<Properties?> propertiesFactory, DateTime createdUtc)
    {
        Version = version;
        HasServerMapping = hasServerMapping;
        Fetched = fetched;
        CreatedUtc = createdUtc;
        _properties = new Lazy<Properties?>(propertiesFactory, LazyThreadSafetyMode.ExecutionAndPublication);
    }

    /// <summary>Monotonically increasing version used to detect field cache entries from older mappings.</summary>
    public long Version { get; }

    /// <summary>Whether the server returned a mapping (as opposed to no mapping being available).</summary>
    public bool HasServerMapping { get; }

    /// <summary>Whether a server mapping fetch has been attempted for this snapshot.</summary>
    public bool Fetched { get; }

    public DateTime CreatedUtc { get; }

    /// <summary>Code and server properties merged once, on first use.</summary>
    public Properties? Properties => _properties.Value;
}

internal enum MappingRefreshResult
{
    /// <summary>No refresh was attempted (no fetch function, or the cache was disposed).</summary>
    Skipped,

    /// <summary>A refresh was warranted but suppressed by a throttle.</summary>
    Throttled,

    /// <summary>
    /// A refresh was already in flight but did not complete within the wait timeout, so the loaded mapping is
    /// known to be stale.
    /// </summary>
    WaitTimedOut,

    /// <summary>The server mapping was reloaded and a new snapshot published.</summary>
    Updated,

    /// <summary>The refresh was attempted but did not produce a usable mapping.</summary>
    Failed
}
