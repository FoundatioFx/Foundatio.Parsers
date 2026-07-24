using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Linq;
using System.Runtime.CompilerServices;
using System.Text;
using System.Threading;
using Elastic.Clients.Elasticsearch;
using Elastic.Clients.Elasticsearch.IndexManagement;
using Elastic.Clients.Elasticsearch.Mapping;
using Foundatio.Parsers.ElasticQueries.Extensions;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Logging.Abstractions;

namespace Foundatio.Parsers.ElasticQueries;

public class ElasticMappingResolver : IDisposable
{
    private static readonly TimeSpan _fetchJoinTimeout = TimeSpan.FromSeconds(5);

    private readonly TypeMapping? _codeMapping;
    private readonly Lazy<Properties?> _inferredCodeProperties;
    private readonly Inferrer? _inferrer;
    private readonly ConcurrentDictionary<string, FieldMapping> _mappingCache = new(StringComparer.Ordinal);
    private readonly SemaphoreSlim _fetchSemaphore = new(1, 1);
    private readonly object _publishLock = new();
    private readonly TimeProvider _timeProvider;
    private readonly ConditionalWeakTable<IProperty, ConcurrentDictionary<string, object>> _propertyMetadata = new();
    private readonly ILogger _logger;

    private MappingSnapshot _snapshot;
    private long _snapshotVersion;
    private long _refreshVersion;
    private long _cachedFieldCount;
    private long _lastFetchTimestamp;
    private long _lastUnmappedFetchTimestamp;
    private long _unmappedRefreshBackoffTicks;
    private long _backoffAppliedForVersion;
    private long _lastSuppressedReloadWarningTimestamp;
    private volatile bool _disposed;

    public static readonly ElasticMappingResolver NullInstance = new(() => null);

    public ElasticMappingResolver(Func<TypeMapping?> getMapping, Inferrer? inferrer = null, TimeProvider? timeProvider = null, ILogger? logger = null)
    {
        GetServerMappingFunc = getMapping;
        _inferrer = inferrer;
        _timeProvider = timeProvider ?? TimeProvider.System;
        _logger = logger ?? NullLogger.Instance;
        _inferredCodeProperties = new Lazy<Properties?>(() => InferCodeProperties(_codeMapping?.Properties), LazyThreadSafetyMode.ExecutionAndPublication);
        _snapshot = CreateSnapshot(null, fetched: false);
    }

    public ElasticMappingResolver(TypeMapping codeMapping, Inferrer inferrer, Func<TypeMapping?> getMapping, TimeProvider? timeProvider = null, ILogger? logger = null)
        : this(getMapping, inferrer, timeProvider, logger)
    {
        _codeMapping = codeMapping;
    }

    /// <summary>
    /// Maximum age of the loaded server mapping before an ordinary resolution will reload it. This also
    /// acts as the ceiling when backing off repeated reloads triggered by fields that cannot be resolved.
    /// </summary>
    public TimeSpan MappingRefreshInterval { get; set; } = TimeSpan.FromMinutes(1);

    /// <summary>
    /// Minimum interval between server mapping reloads that are triggered by a field which could not be
    /// resolved from the loaded mapping. A resolution failure is the strongest available signal that the
    /// index mapping changed (fields created by dynamic templates only exist after the first document
    /// that uses them is indexed), so this is intentionally much shorter than <see cref="MappingRefreshInterval"/>.
    /// Reloads that do not resolve the field back off exponentially up to <see cref="MappingRefreshInterval"/>.
    /// Set to <see cref="TimeSpan.Zero"/> to always reload on an unresolved field.
    /// </summary>
    public TimeSpan UnmappedFieldRefreshInterval { get; set; } = TimeSpan.FromSeconds(5);

    /// <summary>
    /// Approximate upper bound on the number of resolved field mappings held in memory. Field names come
    /// from user supplied queries, so this bounds memory usage when queries reference many distinct
    /// (often non-existent) fields. The cache is cleared once the bound is exceeded. Set to zero or less
    /// to disable caching entirely.
    /// </summary>
    public int MaxCachedFields { get; set; } = 10000;

    /// <summary>
    /// Approximate number of field names currently held in the resolved mapping cache.
    /// </summary>
    public long CachedFieldCount => Interlocked.Read(ref _cachedFieldCount);

    private MappingSnapshot Snapshot => Volatile.Read(ref _snapshot);

    /// <summary>
    /// Clears the cached mapping, forcing a fresh fetch from the server on the next access.
    /// </summary>
    /// <remarks>
    /// Server mappings are reloaded automatically: at most once per <see cref="MappingRefreshInterval"/>
    /// for ordinary resolutions and at most once per <see cref="UnmappedFieldRefreshInterval"/> when a
    /// field cannot be resolved. This method bypasses both throttles and discards the entire field cache,
    /// which is expensive on a large mapping. Prefer <see cref="InvalidateFieldMapping"/> when only a
    /// specific field is known to have changed.
    /// </remarks>
    public void RefreshMapping()
    {
        lock (_publishLock)
        {
            Interlocked.Increment(ref _refreshVersion);
            ClearCache();
            Volatile.Write(ref _snapshot, CreateSnapshot(null, fetched: false));
            Interlocked.Exchange(ref _lastFetchTimestamp, 0);
            Interlocked.Exchange(ref _lastUnmappedFetchTimestamp, 0);
            Interlocked.Exchange(ref _unmappedRefreshBackoffTicks, 0);
        }

        _logger.LogInformation("Mapping refresh triggered");
    }

    /// <summary>
    /// Drops the cached resolution for a single field and allows the next resolution of any unmapped field
    /// to reload the server mapping. Use this when a specific field is known to have just been created,
    /// instead of discarding the whole cache with <see cref="RefreshMapping"/>.
    /// </summary>
    public void InvalidateFieldMapping(string? field)
    {
        if (String.IsNullOrWhiteSpace(field))
            return;

        if (_mappingCache.TryRemove(field!, out _))
            Interlocked.Decrement(ref _cachedFieldCount);

        Interlocked.Exchange(ref _lastUnmappedFetchTimestamp, 0);
        Interlocked.Exchange(ref _unmappedRefreshBackoffTicks, 0);

        _logger.LogTrace("Invalidated field mapping: {Field}", field);
    }

    public FieldMapping? GetMapping(string? field, bool followAlias = false)
    {
        if (String.IsNullOrWhiteSpace(field))
            return null;

        if (GetServerMappingFunc is null && _codeMapping is null)
            throw new InvalidOperationException("No mappings are available.");

        var snapshot = Snapshot;

        if (_mappingCache.TryGetValue(field!, out var cached) && cached.Epoch == snapshot.Version)
        {
            if (cached.Found)
            {
                if (followAlias && cached.Property is FieldAliasProperty cachedAlias)
                {
                    if (_logger.IsEnabled(LogLevel.Trace))
                        _logger.LogTrace("Cached alias mapping: {Field}={FieldPath}:{FieldType}", field, cached.FullPath, cached.Property.Type);

                    return GetMapping(cachedAlias.Path?.Name);
                }

                if (_logger.IsEnabled(LogLevel.Trace))
                    _logger.LogTrace("Cached mapping: {Field}={FieldPath}:{FieldType}", field, cached.FullPath, cached.Property?.Type);

                return cached;
            }

            // A cached miss is the strongest available signal that the server mapping may have changed,
            // so attempt a rate limited reload before trusting it.
            if (ReloadServerMapping(unmappedField: true) != MappingFetchResult.Updated)
            {
                if (_logger.IsEnabled(LogLevel.Trace))
                    _logger.LogTrace("Cached mapping (not found): {Field}=<null>", field);

                return cached;
            }

            return ResolveMapping(field!, followAlias, Snapshot, reloadedForUnmappedField: true);
        }

        return ResolveMapping(field!, followAlias, snapshot, reloadedForUnmappedField: false);
    }

    private FieldMapping ResolveMapping(string field, bool followAlias, MappingSnapshot snapshot, bool reloadedForUnmappedField)
    {
        var lastFetchResult = MappingFetchResult.Skipped;
        bool reloaded = reloadedForUnmappedField;
        bool reloadedForMiss = reloadedForUnmappedField;

        // Load the server mapping the first time one is needed. This deliberately does not arm the
        // unmapped field throttle: a cold start fetch must never suppress the first miss driven reload,
        // otherwise fields created after startup resolve as unmapped until the throttle expires.
        if (!snapshot.Fetched && !reloaded)
        {
            lastFetchResult = ReloadServerMapping(unmappedField: false);
            if (lastFetchResult == MappingFetchResult.Updated)
            {
                snapshot = Snapshot;
                reloaded = true;
            }
        }

        string[] fieldParts = field.Split('.');
        var resolvedFieldName = new StringBuilder();
        var currentProperties = snapshot.Properties;

        for (int depth = 0; depth < fieldParts.Length; depth++)
        {
            string fieldPart = fieldParts[depth];
            IProperty? fieldMapping = null;
            string? resolvedName = null;

            if (currentProperties is not null && currentProperties.TryGetProperty(fieldPart, out fieldMapping))
            {
                // Properties is keyed by property name, so an exact hit means the key name is the field part.
                resolvedName = fieldPart;
            }
            else
            {
                fieldMapping = null;

                // check to see if there is a name match by iterating through the dictionary keys
                if (currentProperties is not null)
                {
                    foreach (var kvp in (IDictionary<PropertyName, IProperty>)currentProperties)
                    {
                        string? propertyName = ResolvePropertyName(kvp.Key);
                        if (propertyName is not null && propertyName.Equals(fieldPart, StringComparison.OrdinalIgnoreCase))
                        {
                            fieldMapping = kvp.Value;
                            resolvedName = propertyName;
                            break;
                        }
                    }
                }

                // The field is unknown to the loaded mapping: reload once in case it was created after the
                // mapping was loaded, then start over from the top against the new mapping.
                if (fieldMapping is null && !reloaded)
                {
                    lastFetchResult = ReloadServerMapping(unmappedField: true);
                    if (lastFetchResult == MappingFetchResult.Updated)
                    {
                        reloaded = true;
                        reloadedForMiss = true;
                        depth = -1;
                        resolvedFieldName.Clear();
                        snapshot = Snapshot;
                        currentProperties = snapshot.Properties;
                        continue;
                    }
                }

                if (fieldMapping is null)
                {
                    if (depth > 0)
                        resolvedFieldName.Append('.');
                    resolvedFieldName.Append(fieldPart);

                    // mapping is not fully resolved, append the rest of the parts unmodified and break
                    for (int i = depth + 1; i < fieldParts.Length; i++)
                    {
                        resolvedFieldName.Append('.');
                        resolvedFieldName.Append(fieldParts[i]);
                    }

                    break;
                }
            }

            if (depth > 0)
                resolvedFieldName.Append('.');
            resolvedFieldName.Append(resolvedName ?? fieldPart);

            if (depth == fieldParts.Length - 1)
            {
                var resolvedMapping = new FieldMapping(resolvedFieldName.ToString(), fieldMapping, snapshot.CreatedUtc, snapshot.Version);
                CacheFieldMapping(field, resolvedMapping, snapshot.Version);

                // A miss driven reload that resolved the field is proof the mapping really had changed:
                // return to the fast base interval so the next schema change is picked up quickly.
                if (reloadedForMiss)
                    Interlocked.Exchange(ref _unmappedRefreshBackoffTicks, 0);

                if (_logger.IsEnabled(LogLevel.Trace))
                    _logger.LogTrace("Resolved mapping: {Field}={FieldPath}:{FieldType}", field, resolvedMapping.FullPath, resolvedMapping.Property?.Type);

                if (followAlias && resolvedMapping.Property is FieldAliasProperty fieldAlias)
                    return GetMapping(fieldAlias.Path?.Name) ?? resolvedMapping;

                return resolvedMapping;
            }

            if (fieldMapping is ObjectProperty objectProperty)
            {
                currentProperties = objectProperty.Properties;
            }
            else if (fieldMapping is NestedProperty nestedProperty)
            {
                currentProperties = nestedProperty.Properties;
            }
            else
            {
                if (fieldMapping is TextProperty textProperty)
                    currentProperties = textProperty.Fields;
                else
                    break;
            }
        }

        // A freshly reloaded mapping that still does not contain the field means the field probably does not
        // exist at all (a typo or a query against a field that was never indexed). Back off so a flood of
        // bogus field names cannot turn every query into a mapping fetch.
        if (reloadedForMiss)
            IncreaseUnmappedRefreshBackoff(snapshot.Version);
        else if (lastFetchResult == MappingFetchResult.Throttled && snapshot.HasServerMapping && ShouldLogSuppressedReload())
            _logger.LogWarning("Unable to resolve mapping for field {Field}. The loaded server mapping is {MappingAge} old and a reload was suppressed by the {RefreshInterval} unmapped field refresh throttle, so this field is being treated as unmapped", field,
                _timeProvider.GetUtcNow().UtcDateTime - snapshot.CreatedUtc, new TimeSpan(GetUnmappedRefreshIntervalTicks()));

        if (_logger.IsEnabled(LogLevel.Trace))
            _logger.LogTrace("Mapping not found: {Field}", field);

        var notFoundMapping = new FieldMapping(resolvedFieldName.ToString(), null, snapshot.CreatedUtc, snapshot.Version);
        CacheFieldMapping(field, notFoundMapping, snapshot.Version);

        return notFoundMapping;
    }

    /// <summary>
    /// Rate limits the suppressed reload warning so a flood of queries against non-existent fields cannot
    /// flood the log. At most one warning is emitted per <see cref="MappingRefreshInterval"/>.
    /// </summary>
    private bool ShouldLogSuppressedReload()
    {
        if (!_logger.IsEnabled(LogLevel.Warning))
            return false;

        long last = Interlocked.Read(ref _lastSuppressedReloadWarningTimestamp);
        if (last != 0 && _timeProvider.GetElapsedTime(last) < MappingRefreshInterval)
            return false;

        return Interlocked.CompareExchange(ref _lastSuppressedReloadWarningTimestamp, _timeProvider.GetTimestamp(), last) == last;
    }

    private string? ResolvePropertyName(PropertyName? key)
    {
        if (key?.Name is null)
            return null;

        return _inferrer is not null ? _inferrer.PropertyName(key) : key.Name;
    }

    private void CacheFieldMapping(string field, FieldMapping mapping, long snapshotVersion)
    {
        int maxCachedFields = MaxCachedFields;
        if (maxCachedFields <= 0)
            return;

        // Do not publish a resolution that was made against a mapping which has already been replaced.
        if (Snapshot.Version != snapshotVersion)
            return;

        if (Interlocked.Read(ref _cachedFieldCount) >= maxCachedFields && !_mappingCache.ContainsKey(field))
        {
            _logger.LogWarning("Field mapping cache exceeded {MaxCachedFields} entries and was cleared", maxCachedFields);
            ClearCache();
        }

        if (_mappingCache.TryAdd(field, mapping))
        {
            Interlocked.Increment(ref _cachedFieldCount);
            return;
        }

        _mappingCache.AddOrUpdate(field, mapping, (_, existing) => existing.Epoch > mapping.Epoch ? existing : mapping);
    }

    private void ClearCache()
    {
        _mappingCache.Clear();
        Interlocked.Exchange(ref _cachedFieldCount, 0);
    }

    public FieldMapping? GetMapping(Field field, bool followAlias = false)
    {
        if (_inferrer is null)
            throw new InvalidOperationException("Unable to resolve Field without inferrer");

        return GetMapping(_inferrer.Field(field), followAlias);
    }

    public IProperty? GetMappingProperty(string? field, bool followAlias = false)
    {
        return GetMapping(field, followAlias)?.Property;
    }

    public IProperty? GetMappingProperty(Field field, bool followAlias = false)
    {
        return GetMapping(field, followAlias)?.Property;
    }

    public string? GetResolvedField(string? field)
    {
        var result = GetMapping(field, true);
        return result?.FullPath ?? field;
    }

    public string GetResolvedField(Field field)
    {
        if (_inferrer is null)
            throw new InvalidOperationException("Unable to resolve Field without inferrer");

        return GetResolvedField(_inferrer.Field(field))!;
    }

    public string? GetSortFieldName(string? field)
    {
        return GetNonAnalyzedFieldName(field, ElasticMapping.SortFieldName);
    }

    public string GetSortFieldName(Field field)
    {
        return GetNonAnalyzedFieldName(GetResolvedField(field), ElasticMapping.SortFieldName)!;
    }

    public string? GetAggregationsFieldName(string? field)
    {
        return GetNonAnalyzedFieldName(field, ElasticMapping.KeywordFieldName);
    }

    public string GetAggregationsFieldName(Field field)
    {
        return GetNonAnalyzedFieldName(field, ElasticMapping.KeywordFieldName)!;
    }

    public string GetNonAnalyzedFieldName(Field field, string? preferredSubField = null)
    {
        return GetNonAnalyzedFieldName(GetResolvedField(field), preferredSubField)!;
    }

    public string? GetNonAnalyzedFieldName(string? field, string? preferredSubField = null)
    {
        if (String.IsNullOrEmpty(field))
            return field;

        var mapping = GetMapping(field, true);

        if (mapping?.Property is null || !IsPropertyAnalyzed(mapping.Property))
            return field;

        var multiFieldProperty = mapping.Property;
        var fields = multiFieldProperty.GetFields();
        if (fields is null || (IDictionary<PropertyName, IProperty>)fields is not { Count: > 0 })
            return mapping.FullPath;

        var nonAnalyzedProperty = fields.OrderByDescending(kvp => kvp.Key.Name == preferredSubField).FirstOrDefault(kvp =>
        {
            if (kvp.Value is KeywordProperty)
                return true;

            if (!IsPropertyAnalyzed(kvp.Value))
                return true;

            return false;
        });

        if (nonAnalyzedProperty.Value is not null)
            return mapping.FullPath + "." + nonAnalyzedProperty.Key.Name;

        return mapping.FullPath;
    }

    public bool IsPropertyAnalyzed(string? field)
    {
        // assume default is analyzed
        if (String.IsNullOrEmpty(field))
            return true;

        var property = GetMapping(field, true);
        if (property is null || !property.Found)
            return false;

        return IsPropertyAnalyzed(property.Property!);
    }

    public bool IsPropertyAnalyzed(IProperty property)
    {
        if (property is TextProperty textProperty)
            return !textProperty.Index.HasValue || textProperty.Index.Value;

        return false;
    }

    public bool IsNestedPropertyType(string? field)
    {
        if (String.IsNullOrEmpty(field))
            return false;

        return GetMappingProperty(field, true) is NestedProperty;
    }

    public bool IsGeoPropertyType(string? field)
    {
        if (String.IsNullOrEmpty(field))
            return false;

        return GetMappingProperty(field, true) is GeoPointProperty;
    }

    public bool IsNumericPropertyType(string? field)
    {
        if (String.IsNullOrEmpty(field))
            return false;

        var property = GetMappingProperty(field, true);
        return property is ByteNumberProperty
            or DoubleNumberProperty
            or FloatNumberProperty
            or HalfFloatNumberProperty
            or IntegerNumberProperty
            or LongNumberProperty
            or ScaledFloatNumberProperty
            or ShortNumberProperty
            or UnsignedLongNumberProperty;
    }

    public bool IsBooleanPropertyType(string? field)
    {
        if (String.IsNullOrEmpty(field))
            return false;

        return GetMappingProperty(field, true) is BooleanProperty;
    }

    public bool IsDatePropertyType(string? field)
    {
        if (String.IsNullOrEmpty(field))
            return false;

        return GetMappingProperty(field, true) is DateProperty or DateNanosProperty;
    }

    public FieldType GetFieldType(string? field)
    {
        if (String.IsNullOrWhiteSpace(field))
            return FieldType.None;

        var property = GetMappingProperty(field, true);

        if (property?.Type is null)
            return FieldType.None;

        return property.Type switch
        {
            "aggregate_metric_double" => FieldType.AggregateMetricDouble,
            "alias" => FieldType.Alias,
            "binary" => FieldType.Binary,
            "boolean" => FieldType.Boolean,
            "byte" => FieldType.Byte,
            "completion" => FieldType.Completion,
            "constant_keyword" => FieldType.ConstantKeyword,
            "counted_keyword" => FieldType.CountedKeyword,
            "date" => FieldType.Date,
            "date_nanos" => FieldType.DateNanos,
            "date_range" => FieldType.DateRange,
            "dense_vector" => FieldType.DenseVector,
            "double" => FieldType.Double,
            "double_range" => FieldType.DoubleRange,
            "flattened" => FieldType.Flattened,
            "float" => FieldType.Float,
            "float_range" => FieldType.FloatRange,
            "geo_point" => FieldType.GeoPoint,
            "geo_shape" => FieldType.GeoShape,
            "half_float" => FieldType.HalfFloat,
            "histogram" => FieldType.Histogram,
            "icu_collation_keyword" => FieldType.IcuCollationKeyword,
            "integer" => FieldType.Integer,
            "integer_range" => FieldType.IntegerRange,
            "ip" => FieldType.Ip,
            "ip_range" => FieldType.IpRange,
            "join" => FieldType.Join,
            "keyword" => FieldType.Keyword,
            "long" or "unsigned_long" => FieldType.Long,
            "long_range" => FieldType.LongRange,
            "match_only_text" or "string" => FieldType.MatchOnlyText,
            "murmur3" => FieldType.Murmur3,
            "nested" => FieldType.Nested,
            "none" => FieldType.None,
            "object" => FieldType.Object,
            "passthrough" => FieldType.Passthrough,
            "percolator" => FieldType.Percolator,
            "point" or "shape" => FieldType.Shape,
            "rank_feature" => FieldType.RankFeature,
            "rank_features" => FieldType.RankFeatures,
            "scaled_float" => FieldType.ScaledFloat,
            "search_as_you_type" => FieldType.SearchAsYouType,
            "semantic_text" => FieldType.SemanticText,
            "short" => FieldType.Short,
            "sparse_vector" => FieldType.SparseVector,
            "text" => FieldType.Text,
            "token_count" => FieldType.TokenCount,
            "version" => FieldType.Version,
            "wildcard" => FieldType.Wildcard,
            _ => FieldType.None,
        };
    }

    private Properties? InferCodeProperties(Properties? codeProperties)
    {
        if (codeProperties is null)
            return null;

        // resolve code mapping property expressions using inferrer
        var inferredProperties = new Properties();

        foreach (var kvp in codeProperties)
        {
            var propertyName = kvp.Key;
            if (_inferrer is not null && (String.IsNullOrEmpty(kvp.Key.Name) || kvp.Value is FieldAliasProperty))
                propertyName = _inferrer.PropertyName(kvp.Key) ?? kvp.Key;

            inferredProperties[propertyName] = kvp.Value;
        }

        if (_inferrer is null)
            return inferredProperties;

        // resolve field alias
        foreach (var kvp in codeProperties)
        {
            if (kvp.Value is not FieldAliasProperty aliasProperty)
                continue;

            var newAliasProperty = new FieldAliasProperty
            {
                Path = _inferrer.Field(aliasProperty.Path!) ?? aliasProperty.Path,
            };
            CopyPropertyMetadata(aliasProperty, newAliasProperty);
            inferredProperties[_inferrer.PropertyName(kvp.Key) ?? kvp.Key] = newAliasProperty;
        }

        return inferredProperties;
    }

    private Properties? MergeCodeAndServerProperties(Properties? codeProperties, Properties? serverProperties)
    {
        return MergeProperties(InferCodeProperties(codeProperties), serverProperties);
    }

    /// <remarks>
    /// Merging mutates the sub-property collections of the server property objects, so the
    /// <c>getMapping</c> callback must return a mapping instance the resolver can take ownership of
    /// (a freshly deserialized <c>GetMapping</c> response, not a cached shared instance). Merging is done
    /// exactly once per snapshot, so a single mapping instance is never merged concurrently.
    /// </remarks>
    private Properties? MergeProperties(Properties? inferredCodeProperties, Properties? serverProperties)
    {
        // no need to merge
        if (inferredCodeProperties is null || serverProperties is null)
            return inferredCodeProperties ?? serverProperties;

        var properties = new Properties();
        foreach (var serverProperty in serverProperties)
        {
            var merged = serverProperty.Value;

            if (inferredCodeProperties.TryGetProperty(serverProperty.Key, out var codeProperty))
            {
                // Copy local metadata from code property to merged property
                CopyPropertyMetadata(codeProperty, merged);

                switch (merged)
                {
                    case ObjectProperty objectProperty:
                        objectProperty.Properties =
                            MergeCodeAndServerProperties((codeProperty as ObjectProperty)?.Properties, objectProperty.Properties);
                        break;
                    case NestedProperty nestedProperty:
                        nestedProperty.Properties =
                            MergeCodeAndServerProperties((codeProperty as NestedProperty)?.Properties, nestedProperty.Properties);
                        break;
                    case TextProperty textProperty:
                        textProperty.Fields = MergeCodeAndServerProperties((codeProperty as TextProperty)?.Fields, textProperty.Fields);
                        break;
                }
            }

            properties.Add(serverProperty.Key, merged);
        }

        foreach (var codeProperty in inferredCodeProperties)
        {
            if (properties.TryGetProperty(codeProperty.Key, out _))
                continue;

            properties.Add(codeProperty.Key, codeProperty.Value);
        }

        return properties;
    }

    private Func<TypeMapping?>? GetServerMappingFunc { get; set; }

    private MappingSnapshot CreateSnapshot(TypeMapping? serverMapping, bool fetched)
    {
        long version = Interlocked.Increment(ref _snapshotVersion);

        // Properties are merged lazily and exactly once per snapshot: merging walks (and mutates) the
        // whole property tree, which is far too expensive to repeat for every field resolution.
        return new MappingSnapshot(version, serverMapping is not null, fetched,
            () => MergeProperties(_inferredCodeProperties.Value, serverMapping?.Properties),
            _timeProvider.GetUtcNow().UtcDateTime);
    }

    private long GetUnmappedRefreshIntervalTicks()
    {
        long baseTicks = Math.Max(UnmappedFieldRefreshInterval.Ticks, 0);
        long maxTicks = Math.Max(MappingRefreshInterval.Ticks, baseTicks);
        long backoffTicks = Interlocked.Read(ref _unmappedRefreshBackoffTicks);

        return Math.Min(Math.Max(baseTicks, backoffTicks), maxTicks);
    }

    private void IncreaseUnmappedRefreshBackoff()
    {
        long baseTicks = Math.Max(UnmappedFieldRefreshInterval.Ticks, 0);
        long maxTicks = Math.Max(MappingRefreshInterval.Ticks, baseTicks);
        long current = Interlocked.Read(ref _unmappedRefreshBackoffTicks);
        if (current >= maxTicks)
            return;

        long next = Math.Min(Math.Max(current, baseTicks) * 2, maxTicks);
        Interlocked.Exchange(ref _unmappedRefreshBackoffTicks, next);
    }

    /// <summary>
    /// Backs off at most once per mapping reload. Many concurrent lookups adopt the result of a single
    /// reload, and without this guard one reload would ratchet the interval all the way to the ceiling.
    /// </summary>
    private void IncreaseUnmappedRefreshBackoff(long snapshotVersion)
    {
        long applied = Interlocked.Read(ref _backoffAppliedForVersion);
        if (applied == snapshotVersion || Interlocked.CompareExchange(ref _backoffAppliedForVersion, snapshotVersion, applied) != applied)
            return;

        IncreaseUnmappedRefreshBackoff();
    }

    private bool IsReloadAllowed(bool unmappedField)
    {
        if (unmappedField)
        {
            long lastUnmappedFetch = Interlocked.Read(ref _lastUnmappedFetchTimestamp);
            return lastUnmappedFetch == 0 || _timeProvider.GetElapsedTime(lastUnmappedFetch) >= new TimeSpan(GetUnmappedRefreshIntervalTicks());
        }

        long lastFetch = Interlocked.Read(ref _lastFetchTimestamp);
        return lastFetch == 0 || _timeProvider.GetElapsedTime(lastFetch) >= MappingRefreshInterval;
    }

    private void RecordReloadAttempt(bool unmappedField)
    {
        long timestamp = _timeProvider.GetTimestamp();
        Interlocked.Exchange(ref _lastFetchTimestamp, timestamp);
        if (unmappedField)
            Interlocked.Exchange(ref _lastUnmappedFetchTimestamp, timestamp);
    }

    /// <summary>
    /// Reloads the server mapping, publishing a new snapshot when it succeeds.
    /// </summary>
    /// <param name="unmappedField">
    /// True when the reload was triggered by a field that could not be resolved. Miss driven reloads use
    /// their own (much shorter, self backing off) throttle so that a cold start fetch cannot suppress them.
    /// </param>
    private MappingFetchResult ReloadServerMapping(bool unmappedField)
    {
        var getServerMapping = GetServerMappingFunc;
        if (getServerMapping is null || _disposed)
            return MappingFetchResult.Skipped;

        if (!IsReloadAllowed(unmappedField))
            return MappingFetchResult.Throttled;

        long versionBeforeWait = Snapshot.Version;

        bool acquired;
        try
        {
            // Join an in-flight fetch instead of silently continuing with a stale mapping. The wait is
            // bounded because the fetch callback is user supplied and does blocking network I/O.
            acquired = _fetchSemaphore.Wait(_fetchJoinTimeout);
        }
        catch (ObjectDisposedException)
        {
            return MappingFetchResult.Skipped;
        }

        if (!acquired)
        {
            _logger.LogWarning("Timed out after {Timeout} joining an in-flight server mapping fetch, continuing with the loaded mapping", _fetchJoinTimeout);
            return MappingFetchResult.Throttled;
        }

        try
        {
            // Another caller finished a fetch while we waited, so adopt its result rather than refetching.
            // A snapshot that has not been fetched means RefreshMapping() ran while we waited, in which case
            // we still have to do the fetch ourselves.
            var snapshotAfterWait = Snapshot;
            if (snapshotAfterWait.Version != versionBeforeWait && snapshotAfterWait.Fetched)
                return MappingFetchResult.Updated;

            if (!IsReloadAllowed(unmappedField))
                return MappingFetchResult.Throttled;

            long refreshVersion = Interlocked.Read(ref _refreshVersion);

            TypeMapping? newMapping;
            try
            {
                newMapping = getServerMapping();
            }
            catch (Exception ex) when (ex is not OutOfMemoryException and not StackOverflowException)
            {
                // Record the attempt so a failing cluster is not retried for every unresolved field.
                RecordReloadAttempt(unmappedField);
                if (unmappedField)
                    IncreaseUnmappedRefreshBackoff();

                _logger.LogError(ex, "Error getting server mapping: {Message}", ex.Message);
                return MappingFetchResult.Failed;
            }

            var current = Snapshot;
            if (newMapping is null && current.Fetched && !current.HasServerMapping)
            {
                // Nothing changed, so keep the current snapshot (and its resolved field cache) intact.
                RecordReloadAttempt(unmappedField);
                if (unmappedField)
                    IncreaseUnmappedRefreshBackoff();

                return MappingFetchResult.Failed;
            }

            lock (_publishLock)
            {
                // A RefreshMapping() during the fetch means this result may predate the schema change the
                // caller knows about, so discard it and let the next resolution fetch again.
                if (Interlocked.Read(ref _refreshVersion) != refreshVersion)
                    return MappingFetchResult.Failed;

                // Clear before publishing so resolutions made against the new snapshot are not discarded.
                ClearCache();
                Volatile.Write(ref _snapshot, CreateSnapshot(newMapping, fetched: true));
                RecordReloadAttempt(unmappedField);
            }

            _logger.LogInformation("Got server mapping");

            return MappingFetchResult.Updated;
        }
        finally
        {
            try
            {
                _fetchSemaphore.Release();
            }
            catch (ObjectDisposedException)
            {
                // the resolver was disposed while the fetch was in flight
            }
        }
    }

    public static ElasticMappingResolver Create<T>(Action<TypeMappingDescriptor<T>> mappingBuilder, ElasticsearchClient client, ILogger? logger = null) where T : class
    {
        logger ??= NullLogger.Instance;

        return Create(mappingBuilder, client.Infer, () =>
        {
            var response = client.Indices.GetMapping(new GetMappingRequest(Indices.Index<T>()));
            logger.LogTrace("GetMapping: {Request}", response.GetRequest(false, true));

            // use first returned mapping because index could have been an index alias
            var mapping = response.Mappings.Values.FirstOrDefault()?.Mappings;
            return mapping;
        }, logger);
    }

    public static ElasticMappingResolver Create<T>(Action<TypeMappingDescriptor<T>> mappingBuilder, ElasticsearchClient client, string index, ILogger? logger = null) where T : class
    {
        logger ??= NullLogger.Instance;

        return Create(mappingBuilder, client.Infer, () =>
        {
            var response = client.Indices.GetMapping(new GetMappingRequest(index));
            logger.LogTrace("GetMapping: {Request}", response.GetRequest(false, true));

            // use first returned mapping because index could have been an index alias
            var mapping = response.Mappings.Values.FirstOrDefault()?.Mappings;
            return mapping;
        }, logger);
    }

    public static ElasticMappingResolver Create<T>(Action<TypeMappingDescriptor<T>> mappingBuilder, Inferrer inferrer, Func<TypeMapping?> getMapping, ILogger? logger = null) where T : class
    {
        var descriptor = new TypeMappingDescriptor<T>();
        mappingBuilder(descriptor);
        return new ElasticMappingResolver(descriptor, inferrer, getMapping, logger: logger);
    }

    public static ElasticMappingResolver Create<T>(ElasticsearchClient client, ILogger? logger = null)
    {
        logger ??= NullLogger.Instance;

        return Create(() =>
        {
            var response = client.Indices.GetMapping(new GetMappingRequest(Indices.Index<T>()));
            logger.LogTrace("GetMapping: {Request}", response.GetRequest(false, true));

            // use first returned mapping because index could have been an index alias
            var mapping = response.Mappings.Values.FirstOrDefault()?.Mappings;
            return mapping;
        }, client.Infer, logger);
    }

    public static ElasticMappingResolver Create(ElasticsearchClient client, string index, ILogger? logger = null)
    {
        logger ??= NullLogger.Instance;

        return Create(() =>
        {
            var response = client.Indices.GetMapping(new GetMappingRequest(index));
            logger.LogTrace("GetMapping: {Request}", response.GetRequest(false, true));

            // use first returned mapping because index could have been an index alias
            var mapping = response.Mappings.Values.FirstOrDefault()?.Mappings;
            return mapping;
        }, client.Infer, logger);
    }

    public static ElasticMappingResolver Create(Func<TypeMapping?> getMapping, Inferrer? inferrer, ILogger? logger = null)
    {
        return new ElasticMappingResolver(getMapping, inferrer, logger: logger);
    }


    public IDictionary<string, object>? GetPropertyMetadata(IProperty property)
    {
        if (property is null)
            return null;

        return _propertyMetadata.GetOrCreateValue(property);
    }

    public T? GetPropertyMetadataValue<T>(IProperty property, string key, T? defaultValue = default)
    {
        var metadata = GetPropertyMetadata(property);
        if (metadata is null || !metadata.TryGetValue(key, out var value))
            return defaultValue;

        if (value is T typedValue)
            return typedValue;

        try
        {
            return (T)Convert.ChangeType(value, typeof(T));
        }
        catch (Exception ex) when (ex is InvalidCastException or FormatException or OverflowException)
        {
            return defaultValue;
        }
    }

    public void SetPropertyMetadataValue(IProperty property, string key, object value)
    {
        if (property is null)
            return;

        var metadata = _propertyMetadata.GetOrCreateValue(property);
        metadata[key] = value;
    }

    public void CopyPropertyMetadata(IProperty source, IProperty target)
    {
        if (source is null || target is null)
            return;

        if (!_propertyMetadata.TryGetValue(source, out var sourceMetadata))
            return;

        var targetMetadata = _propertyMetadata.GetOrCreateValue(target);
        foreach (var kvp in sourceMetadata)
            targetMetadata[kvp.Key] = kvp.Value;
    }

    public void Dispose()
    {
        if (_disposed)
            return;

        _disposed = true;
        _fetchSemaphore.Dispose();
    }

    /// <summary>
    /// An immutable point-in-time view of the mapping. Published by reference swap so that readers never
    /// need a lock and always observe a consistent mapping plus version pair.
    /// </summary>
    private sealed class MappingSnapshot
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

    private enum MappingFetchResult
    {
        /// <summary>No fetch was attempted (no fetch function, disposed, or already reloaded).</summary>
        Skipped,

        /// <summary>A fetch was warranted but suppressed by a refresh throttle.</summary>
        Throttled,

        /// <summary>The server mapping was reloaded and a new snapshot published.</summary>
        Updated,

        /// <summary>The fetch was attempted but did not produce a usable mapping.</summary>
        Failed
    }
}

public class FieldMapping
{
    public FieldMapping(string path, IProperty? property, DateTime? serverMapTime, long epoch = 0)
    {
        FullPath = path;
        Property = property;
        ServerMapTime = serverMapTime;
        Epoch = epoch;
    }

    public bool Found => Property is not null;
    public string FullPath { get; private set; }
    public IProperty? Property { get; private set; }
    public DateTime Date { get; private set; } = DateTime.UtcNow;

    /// <summary>When the mapping this resolution was made against was loaded.</summary>
    internal DateTime? ServerMapTime { get; private set; }

    /// <summary>Version of the mapping snapshot this resolution was made against.</summary>
    internal long Epoch { get; private set; }
}
