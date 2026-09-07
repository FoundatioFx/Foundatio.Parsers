using System;
using System.Collections.Concurrent;
using System.Threading;
using System.Threading.Tasks;
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
    private readonly Func<CancellationToken, Task<TypeMapping?>>? _getServerMappingAsync;
    private readonly Func<TypeMapping?, MergedProperties?> _merge;
    private readonly TimeProvider _timeProvider;
    private readonly ILogger _logger;
    private readonly object _stateLock = new();
    private readonly CancellationTokenSource _lifetimeCancellation = new();

    private MappingSnapshot _snapshot;
    private MappingLoadOperation? _inFlightLoad;
    private long _snapshotVersion;
    private long _lastLoadAttemptTimestamp;
    private int _hasLoadAttempt;
    private bool _disposed;

    public MappingCache(Func<TypeMapping?>? getServerMapping, Func<TypeMapping?, MergedProperties?> merge, TimeProvider timeProvider, ILogger logger)
        : this(getServerMapping, null, merge, timeProvider, logger)
    {
    }

    public MappingCache(Func<CancellationToken, Task<TypeMapping?>>? getServerMappingAsync, Func<TypeMapping?, MergedProperties?> merge, TimeProvider timeProvider, ILogger logger)
        : this(null, getServerMappingAsync, merge, timeProvider, logger)
    {
    }

    public MappingCache(Func<TypeMapping?>? getServerMapping, Func<CancellationToken, Task<TypeMapping?>>? getServerMappingAsync,
        Func<TypeMapping?, MergedProperties?> merge, TimeProvider timeProvider, ILogger logger)
    {
        _getServerMapping = getServerMapping;
        _getServerMappingAsync = getServerMappingAsync;
        _merge = merge;
        _timeProvider = timeProvider;
        _logger = logger;
        _snapshot = CreateSnapshot(null, fetched: false);
    }

    public TimeSpan UnmappedFieldRefreshInterval { get; set; } = TimeSpan.FromSeconds(5);

    public Func<TypeMapping, string?>? ServerMappingRevisionResolver { get; set; }

    /// <summary>
    /// How long a load waits to join one already in flight. Validated by the resolver to be positive and
    /// within the range <see cref="Task.WaitAsync(TimeSpan, CancellationToken)"/> accepts, so it is passed
    /// through unclamped.
    /// </summary>
    public TimeSpan RefreshWaitTimeout { get; set; } = TimeSpan.FromSeconds(30);

    public bool HasServerMappingFunc => _getServerMapping is not null || _getServerMappingAsync is not null;

    /// <summary>The currently published mapping. Never null; read without locking.</summary>
    public MappingSnapshot Current => Volatile.Read(ref _snapshot);

    /// <summary>
    /// Discards the loaded mapping, every field resolved from it, and the load throttle, so the next
    /// resolution loads immediately.
    /// </summary>
    public void Reset()
    {
        MappingLoadOperation? supersededLoad;
        lock (_stateLock)
        {
            supersededLoad = _inFlightLoad;
            _inFlightLoad = null;
            ClearThrottle();
            Volatile.Write(ref _snapshot, CreateSnapshot(null, fetched: false));
        }

        supersededLoad?.SetResult(MappingRefreshResult.Superseded);
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

    public ValueTask<MappingRefreshResult> LoadInitialAsync(MappingSnapshot observedSnapshot, CancellationToken cancellationToken = default) =>
        LoadAsync(observedSnapshot, armThrottleOnSuccess: false, cancellationToken);

    /// <summary>
    /// Reloads the mapping because a field could not be resolved from the loaded one. A resolution failure is
    /// the strongest available signal that the mapping changed, but it is also caller-triggered, so it is rate
    /// limited by <see cref="UnmappedFieldRefreshInterval"/>.
    /// </summary>
    public MappingRefreshResult ReloadForMissingField(MappingSnapshot observedSnapshot) => Load(observedSnapshot, armThrottleOnSuccess: true);

    public ValueTask<MappingRefreshResult> ReloadForMissingFieldAsync(MappingSnapshot observedSnapshot, CancellationToken cancellationToken = default) =>
        LoadAsync(observedSnapshot, armThrottleOnSuccess: true, cancellationToken);

    private MappingRefreshResult Load(MappingSnapshot observedSnapshot, bool armThrottleOnSuccess)
    {
        while (true)
        {
            if (!TryGetOrCreateLoad(observedSnapshot, armThrottleOnSuccess, preferAsync: false,
                out var operation, out bool isOwner, out var immediateResult))
                return immediateResult;

            if (isOwner)
                StartLoad(operation!);

            MappingRefreshResult result;
            if (isOwner)
            {
                result = operation!.Task.GetAwaiter().GetResult();
            }
            else if (!operation!.Task.Wait(RefreshWaitTimeout))
            {
                return HasNewerMapping(observedSnapshot.Version) ? MappingRefreshResult.Updated : MappingRefreshResult.WaitTimedOut;
            }
            else
            {
                result = operation.Task.GetAwaiter().GetResult();
            }

            if (result != MappingRefreshResult.Superseded)
                return result;

            if (observedSnapshot.Version == operation.ExpectedVersion)
                return MappingRefreshResult.Unavailable;

            observedSnapshot = Current;
            armThrottleOnSuccess = armThrottleOnSuccess && observedSnapshot.Fetched;
        }
    }

    private ValueTask<MappingRefreshResult> LoadAsync(MappingSnapshot observedSnapshot, bool armThrottleOnSuccess, CancellationToken cancellationToken)
    {
        if (cancellationToken.IsCancellationRequested)
            return ValueTask.FromCanceled<MappingRefreshResult>(cancellationToken);

        if (!TryGetOrCreateLoad(observedSnapshot, armThrottleOnSuccess, preferAsync: true,
            out var operation, out bool isOwner, out var immediateResult))
            return ValueTask.FromResult(immediateResult);

        if (isOwner)
            StartLoad(operation!);

        if (operation!.Task.IsCompletedSuccessfully)
        {
            if (operation.Task.Result != MappingRefreshResult.Superseded)
                return ValueTask.FromResult(operation.Task.Result);

            if (observedSnapshot.Version == operation.ExpectedVersion)
                return ValueTask.FromResult(MappingRefreshResult.Unavailable);

            var snapshot = Current;
            return LoadAsync(snapshot, armThrottleOnSuccess && snapshot.Fetched, cancellationToken);
        }

        return AwaitLoadAsync(operation, isOwner, observedSnapshot.Version, armThrottleOnSuccess, cancellationToken);
    }

    private async ValueTask<MappingRefreshResult> AwaitLoadAsync(MappingLoadOperation operation, bool isOwner,
        long observedVersion, bool armThrottleOnSuccess, CancellationToken cancellationToken)
    {
        MappingRefreshResult result;
        try
        {
            result = isOwner
                ? await operation.Task.WaitAsync(cancellationToken).AnyContext()
                : await operation.Task.WaitAsync(RefreshWaitTimeout, cancellationToken).AnyContext();
        }
        catch (TimeoutException)
        {
            return HasNewerMapping(observedVersion) ? MappingRefreshResult.Updated : MappingRefreshResult.WaitTimedOut;
        }

        if (result != MappingRefreshResult.Superseded)
            return result;

        if (observedVersion == operation.ExpectedVersion)
            return MappingRefreshResult.Unavailable;

        var snapshot = Current;
        return await LoadAsync(snapshot, armThrottleOnSuccess && snapshot.Fetched, cancellationToken).AnyContext();
    }

    private bool TryGetOrCreateLoad(MappingSnapshot observedSnapshot, bool armThrottleOnSuccess, bool preferAsync,
        out MappingLoadOperation? operation, out bool isOwner, out MappingRefreshResult result)
    {
        lock (_stateLock)
        {
            operation = null;
            isOwner = false;

            if ((_getServerMapping is null && _getServerMappingAsync is null) || _disposed)
            {
                result = MappingRefreshResult.Unavailable;
                return false;
            }

            if (HasNewerMapping(observedSnapshot.Version))
            {
                result = MappingRefreshResult.Updated;
                return false;
            }

            if (_inFlightLoad is not null)
            {
                operation = _inFlightLoad;
                result = default;
                return true;
            }

            if (!IsLoadAllowed())
            {
                result = MappingRefreshResult.Throttled;
                return false;
            }

            operation = new MappingLoadOperation(Current.Version, armThrottleOnSuccess, preferAsync);
            _inFlightLoad = operation;
            isOwner = true;
            result = default;
            return true;
        }
    }

    private void StartLoad(MappingLoadOperation operation)
    {
        if (operation.PreferAsync && _getServerMappingAsync is not null)
        {
            // AnyContext protects continuations owned by this library, but an async loader can still capture the
            // caller's context before it returns its Task. Isolate that boundary when a synchronous caller could
            // otherwise block the only thread while joining this shared load.
            if (SynchronizationContext.Current is not null || TaskScheduler.Current != TaskScheduler.Default)
                _ = Task.Run(() => FetchAsync(operation));
            else
                _ = FetchAsync(operation);
            return;
        }

        if (!operation.PreferAsync && _getServerMapping is not null)
        {
            Fetch(operation);
            return;
        }

        if (_getServerMappingAsync is not null)
        {
            // A synchronous caller cannot safely block on an async callback started on its own synchronization
            // context. Isolate only this compatibility path on the thread pool; normal async callers invoke the
            // callback directly above.
            _ = Task.Run(() => FetchAsync(operation));
            return;
        }

        Fetch(operation);
    }

    private void Fetch(MappingLoadOperation operation)
    {
        TypeMapping? mapping;
        string? revision;
        try
        {
            mapping = _getServerMapping!();
            revision = mapping is null ? null : ServerMappingRevisionResolver?.Invoke(mapping);
        }
        catch (Exception ex)
        {
            if (ex is OutOfMemoryException or StackOverflowException)
            {
                CompleteFaultedLoad(operation, ex);
                return;
            }

            CompleteFailedLoad(operation, ex);
            return;
        }

        FinalizeLoad(operation, mapping, revision);
    }

    private async Task FetchAsync(MappingLoadOperation operation)
    {
        TypeMapping? mapping;
        string? revision;
        try
        {
            mapping = await _getServerMappingAsync!(_lifetimeCancellation.Token).AnyContext();
            revision = mapping is null ? null : ServerMappingRevisionResolver?.Invoke(mapping);
        }
        catch (Exception ex)
        {
            if (ex is OutOfMemoryException or StackOverflowException)
            {
                CompleteFaultedLoad(operation, ex);
                return;
            }

            CompleteFailedLoad(operation, ex);
            return;
        }

        FinalizeLoad(operation, mapping, revision);
    }

    private void CompleteFailedLoad(MappingLoadOperation operation, Exception exception)
    {
        FinalizeLoad(operation, null);
        if (exception is not OperationCanceledException || !_lifetimeCancellation.IsCancellationRequested)
            _logger.LogError(exception, "Error getting server mapping: {Message}", exception.Message);
    }

    private void CompleteFaultedLoad(MappingLoadOperation operation, Exception exception)
    {
        lock (_stateLock)
        {
            operation.SetException(exception);
            if (ReferenceEquals(_inFlightLoad, operation))
                _inFlightLoad = null;
        }
    }

    private void FinalizeLoad(MappingLoadOperation operation, TypeMapping? mapping, string? revision = null)
    {
        MappingRefreshResult result;

        lock (_stateLock)
        {
            if (_disposed || Current.Version != operation.ExpectedVersion)
            {
                result = MappingRefreshResult.Superseded;
            }
            else if (mapping is null)
            {
                RecordLoadAttempt();
                result = MappingRefreshResult.Unavailable;
            }
            else
            {
                Volatile.Write(ref _snapshot, CreateSnapshot(mapping, fetched: true, revision));
                if (operation.ArmThrottleOnSuccess)
                    RecordLoadAttempt();
                result = MappingRefreshResult.Updated;
            }

            operation.SetResult(result);
            if (ReferenceEquals(_inFlightLoad, operation))
                _inFlightLoad = null;
        }

        if (result == MappingRefreshResult.Updated)
            _logger.LogInformation("Got server mapping");
    }

    private bool HasNewerMapping(long version)
    {
        var snapshot = Current;
        return snapshot.Version != version && snapshot.Fetched;
    }

    private bool IsLoadAllowed()
    {
        return _hasLoadAttempt == 0
            || _timeProvider.GetElapsedTime(_lastLoadAttemptTimestamp) >= UnmappedFieldRefreshInterval;
    }

    private void RecordLoadAttempt()
    {
        _lastLoadAttemptTimestamp = _timeProvider.GetTimestamp();
        _hasLoadAttempt = 1;
    }

    private MappingSnapshot CreateSnapshot(TypeMapping? serverMapping, bool fetched, string? revision = null)
    {
        long version = Interlocked.Increment(ref _snapshotVersion);
        DateTime createdUtc = _timeProvider.GetUtcNow().UtcDateTime;
        if (fetched && !String.IsNullOrEmpty(revision) && String.Equals(revision, Current.ServerRevision, StringComparison.Ordinal))
            return Current.WithVersion(version, createdUtc);

        return new MappingSnapshot(version, serverMapping, fetched, _merge, createdUtc, revision);
    }

    public void Dispose()
    {
        lock (_stateLock)
            _disposed = true;

        _lifetimeCancellation.Cancel();
    }

    private sealed class MappingLoadOperation
    {
        private readonly TaskCompletionSource<MappingRefreshResult> _completion =
            new(TaskCreationOptions.RunContinuationsAsynchronously);

        public MappingLoadOperation(long expectedVersion, bool armThrottleOnSuccess, bool preferAsync)
        {
            ExpectedVersion = expectedVersion;
            ArmThrottleOnSuccess = armThrottleOnSuccess;
            PreferAsync = preferAsync;
        }

        public long ExpectedVersion { get; }

        public bool ArmThrottleOnSuccess { get; }

        public bool PreferAsync { get; }

        public Task<MappingRefreshResult> Task => _completion.Task;

        public void SetResult(MappingRefreshResult result) => _completion.TrySetResult(result);

        public void SetException(Exception exception) => _completion.TrySetException(exception);
    }
}

/// <summary>
/// A point-in-time view of the mapping, its merged property tree, and successful field resolutions made
/// against it. Publishing a new snapshot atomically invalidates everything derived from the old mapping.
/// </summary>
internal sealed class MappingSnapshot
{
    private readonly Lazy<MergedProperties?> _properties;
    private readonly ConcurrentDictionary<string, FieldMapping> _fields;

    public MappingSnapshot(long version, TypeMapping? serverMapping, bool fetched, Func<TypeMapping?, MergedProperties?> merge,
        DateTime createdUtc, string? serverRevision)
    {
        Version = version;
        HasServerMapping = serverMapping is not null;
        Fetched = fetched;
        CreatedUtc = createdUtc;
        ServerRevision = serverRevision;
        _properties = new Lazy<MergedProperties?>(() => merge(serverMapping), LazyThreadSafetyMode.ExecutionAndPublication);
        _fields = new ConcurrentDictionary<string, FieldMapping>(StringComparer.Ordinal);
    }

    private MappingSnapshot(MappingSnapshot previous, long version, DateTime createdUtc)
    {
        Version = version;
        HasServerMapping = previous.HasServerMapping;
        Fetched = previous.Fetched;
        CreatedUtc = createdUtc;
        ServerRevision = previous.ServerRevision;
        _properties = previous._properties;
        _fields = previous._fields;
    }

    public long Version { get; }

    public bool HasServerMapping { get; }

    public bool Fetched { get; }

    public DateTime CreatedUtc { get; }

    public string? ServerRevision { get; }

    public MappingSnapshot WithVersion(long version, DateTime createdUtc) => new(this, version, createdUtc);

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
        if (Fetched && mapping.Found)
            _fields.TryAdd(mapping.FullPath, mapping);
    }
}

internal enum MappingRefreshResult
{
    Unavailable,
    Throttled,
    WaitTimedOut,
    Superseded,
    Updated
}
