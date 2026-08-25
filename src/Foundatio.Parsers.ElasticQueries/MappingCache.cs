using System;
using System.Collections.Concurrent;
using System.Threading;
using Elastic.Clients.Elasticsearch.Mapping;
using Microsoft.Extensions.Logging;

namespace Foundatio.Parsers.ElasticQueries;

/// <summary>
/// Loads the server mapping and publishes it as an immutable <see cref="MappingSnapshot"/>. Owns only when to
/// load and how concurrent loads are coalesced.
/// </summary>
/// <remarks>
/// Split out of <see cref="ElasticMappingResolver"/> so load policy can be reasoned about independently of
/// field name resolution. This type never interprets the mapping; deriving the merged property tree is the
/// resolver's job, supplied as a pure function and applied lazily by the snapshot.
/// </remarks>
internal sealed class MappingCache : IDisposable
{
    private readonly Func<TypeMapping?>? _getServerMapping;
    private readonly Func<TypeMapping?, MergedProperties?> _merge;
    private readonly TimeProvider _timeProvider;
    private readonly ILogger _logger;
    private readonly SemaphoreSlim _loadSemaphore = new(1, 1);
    private readonly object _publishLock = new();

    private MappingSnapshot _snapshot;
    private long _snapshotVersion;
    private long _lastLoadAttemptTimestamp;
    private int _hasLoadAttempt;
    private volatile bool _disposed;

    public MappingCache(Func<TypeMapping?>? getServerMapping, Func<TypeMapping?, MergedProperties?> merge, TimeProvider timeProvider, ILogger logger)
    {
        _getServerMapping = getServerMapping;
        _merge = merge;
        _timeProvider = timeProvider;
        _logger = logger;
        _snapshot = CreateSnapshot(null, fetched: false);
    }

    public TimeSpan UnmappedFieldRefreshInterval { get; set; } = TimeSpan.FromSeconds(5);

    /// <summary>
    /// How long a load waits to join one already in flight. Validated by the resolver to be positive and
    /// within the range <see cref="SemaphoreSlim"/> accepts, so it is passed through unclamped.
    /// </summary>
    public TimeSpan RefreshWaitTimeout { get; set; } = TimeSpan.FromSeconds(30);

    public bool HasServerMappingFunc => _getServerMapping is not null;

    /// <summary>The currently published mapping. Never null; read without locking.</summary>
    public MappingSnapshot Current => Volatile.Read(ref _snapshot);

    /// <summary>
    /// Discards the loaded mapping, every field resolved from it, and the load throttle, so the next
    /// resolution loads immediately.
    /// </summary>
    public void Reset()
    {
        lock (_publishLock)
        {
            ClearThrottle();
            Volatile.Write(ref _snapshot, CreateSnapshot(null, fetched: false));
        }
    }

    /// <summary>Clears the load throttle so the next resolution that needs a mapping can load immediately.</summary>
    private void ClearThrottle()
    {
        Interlocked.Exchange(ref _lastLoadAttemptTimestamp, 0);
        Volatile.Write(ref _hasLoadAttempt, 0);
    }

    /// <summary>
    /// Loads the mapping for the first time. A successful cold start never arms the unmapped field throttle:
    /// loading an existing mapping must not suppress discovery of a field created immediately afterwards.
    /// A failed one does, so an unreachable cluster is not retried on every lookup.
    /// </summary>
    public MappingRefreshResult LoadInitial(MappingSnapshot observedSnapshot) => Load(observedSnapshot, armThrottleOnSuccess: false);

    /// <summary>
    /// Reloads the mapping because a field could not be resolved from the loaded one. A resolution failure is
    /// the strongest available signal that the mapping changed, but it is also caller-triggered, so it is rate
    /// limited by <see cref="UnmappedFieldRefreshInterval"/>.
    /// </summary>
    public MappingRefreshResult ReloadForMissingField(MappingSnapshot observedSnapshot) => Load(observedSnapshot, armThrottleOnSuccess: true);

    private MappingRefreshResult Load(MappingSnapshot observedSnapshot, bool armThrottleOnSuccess)
    {
        var getServerMapping = _getServerMapping;
        if (getServerMapping is null || _disposed)
            return MappingRefreshResult.Unavailable;

        if (HasNewerMapping(observedSnapshot.Version))
            return MappingRefreshResult.Updated;

        bool acquired;
        try
        {
            // Wait for an in-flight load rather than continuing with a stale mapping or issuing a second
            // fetch. That load is exactly the work this resolution needs, so waiting is never more expensive
            // than doing it here. With no load in flight this acquires immediately. The wait is bounded
            // because the callback is user supplied and does blocking network I/O, and an unresponsive
            // cluster must not pin request threads forever.
            acquired = _loadSemaphore.Wait(RefreshWaitTimeout);
        }
        catch (ObjectDisposedException)
        {
            return MappingRefreshResult.Unavailable;
        }

        if (!acquired)
        {
            // The in-flight load may have published while we were giving up, in which case adopt it.
            return HasNewerMapping(observedSnapshot.Version) ? MappingRefreshResult.Updated : MappingRefreshResult.WaitTimedOut;
        }

        try
        {
            // Another caller published while we waited, so adopt its result rather than refetching. A snapshot
            // that has not been fetched means Reset() ran while we waited, so we still have to load.
            if (HasNewerMapping(observedSnapshot.Version))
                return MappingRefreshResult.Updated;

            // The throttle only gates issuing a new fetch; a caller whose field is missing still joins any
            // load that was already running above.
            if (!IsLoadAllowed())
                return MappingRefreshResult.Throttled;

            return Fetch(getServerMapping, armThrottleOnSuccess, Current.Version);
        }
        finally
        {
            try
            {
                _loadSemaphore.Release();
            }
            catch (ObjectDisposedException)
            {
                // the resolver was disposed while the user supplied callback was running
            }
        }
    }

    private MappingRefreshResult Fetch(Func<TypeMapping?> getServerMapping, bool armThrottleOnSuccess, long expectedVersion)
    {
        TypeMapping? newMapping;
        try
        {
            newMapping = getServerMapping();
        }
        catch (Exception ex) when (ex is not OutOfMemoryException and not StackOverflowException)
        {
            // Record the attempt so a failing cluster is not retried for every unresolved field. The mapping
            // is deliberately left unfetched so a failed cold start is retried once the interval elapses.
            RecordAttemptUnlessSuperseded(expectedVersion);
            _logger.LogError(ex, "Error getting server mapping: {Message}", ex.Message);
            return MappingRefreshResult.Unavailable;
        }

        if (newMapping is null)
        {
            // Keep the last known good mapping and its resolved fields rather than regressing to no mapping.
            RecordAttemptUnlessSuperseded(expectedVersion);
            return MappingRefreshResult.Unavailable;
        }

        lock (_publishLock)
        {
            // A Reset() during the fetch means this result may predate the change the caller knows about, so
            // discard it and let the next resolution load again.
            if (Current.Version != expectedVersion)
                return MappingRefreshResult.Unavailable;

            Volatile.Write(ref _snapshot, CreateSnapshot(newMapping, fetched: true));
            if (armThrottleOnSuccess)
                RecordLoadAttempt();
        }

        _logger.LogInformation("Got server mapping");
        return MappingRefreshResult.Updated;
    }

    private bool HasNewerMapping(long version)
    {
        var snapshot = Current;
        return snapshot.Version != version && snapshot.Fetched;
    }

    private bool IsLoadAllowed()
    {
        // A separate flag rather than a timestamp sentinel: TimeProvider.GetTimestamp can legitimately return
        // zero, and nudging the stored value off zero would shorten the very first interval.
        return Volatile.Read(ref _hasLoadAttempt) == 0
            || _timeProvider.GetElapsedTime(Interlocked.Read(ref _lastLoadAttemptTimestamp)) >= UnmappedFieldRefreshInterval;
    }

    /// <summary>
    /// Arms the throttle unless an explicit refresh landed during the fetch, in which case the caller asked
    /// for a reload after this attempt started and must not be made to wait out an interval for it.
    /// </summary>
    private void RecordAttemptUnlessSuperseded(long expectedVersion)
    {
        lock (_publishLock)
        {
            if (Current.Version == expectedVersion)
                RecordLoadAttempt();
        }
    }

    private void RecordLoadAttempt()
    {
        Interlocked.Exchange(ref _lastLoadAttemptTimestamp, _timeProvider.GetTimestamp());
        Volatile.Write(ref _hasLoadAttempt, 1);
    }

    private MappingSnapshot CreateSnapshot(TypeMapping? serverMapping, bool fetched)
    {
        long version = Interlocked.Increment(ref _snapshotVersion);
        return new MappingSnapshot(version, serverMapping, fetched, _merge, _timeProvider.GetUtcNow().UtcDateTime);
    }

    public void Dispose()
    {
        if (_disposed)
            return;

        _disposed = true;
        _loadSemaphore.Dispose();
    }
}

/// <summary>
/// A point-in-time view of the mapping, its merged property tree, and successful field resolutions made
/// against it. Publishing a new snapshot atomically invalidates everything derived from the old mapping.
/// </summary>
internal sealed class MappingSnapshot
{
    private readonly Lazy<MergedProperties?> _properties;
    private readonly ConcurrentDictionary<string, FieldMapping> _fields = new(StringComparer.Ordinal);

    public MappingSnapshot(long version, TypeMapping? serverMapping, bool fetched, Func<TypeMapping?, MergedProperties?> merge, DateTime createdUtc)
    {
        Version = version;
        HasServerMapping = serverMapping is not null;
        Fetched = fetched;
        CreatedUtc = createdUtc;
        _properties = new Lazy<MergedProperties?>(() => merge(serverMapping), LazyThreadSafetyMode.ExecutionAndPublication);
    }

    public long Version { get; }

    public bool HasServerMapping { get; }

    public bool Fetched { get; }

    public DateTime CreatedUtc { get; }

    public MergedProperties? Properties => _properties.Value;

    public bool TryGetField(string field, out FieldMapping mapping) => _fields.TryGetValue(field, out mapping!);

    /// <summary>
    /// Memoizes successful resolutions by canonical path. Keying by path keeps this snapshot's cache bounded
    /// by the mapping itself no matter how many distinct spellings callers ask for. Unknown names are caller
    /// controlled and remain uncached so they can drive the bounded mapping-refresh path without growing
    /// process state.
    /// </summary>
    public void CacheField(FieldMapping mapping)
    {
        if (mapping.Found)
            _fields.TryAdd(mapping.FullPath, mapping);
    }
}

internal enum MappingRefreshResult
{
    Unavailable,
    Throttled,
    WaitTimedOut,
    Updated
}
