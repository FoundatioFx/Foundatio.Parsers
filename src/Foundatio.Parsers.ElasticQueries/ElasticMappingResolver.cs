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
    private readonly TypeMapping? _codeMapping;
    private readonly Lazy<Properties?> _inferredCodeProperties;
    private readonly Inferrer? _inferrer;
    private readonly MappingCache _cache;
    private readonly ConditionalWeakTable<IProperty, ConcurrentDictionary<string, object>> _propertyMetadata = new();
    private readonly ConditionalWeakTable<IProperty, Properties> _mergedChildProperties = new();
    private readonly TimeProvider _timeProvider;
    private readonly ILogger _logger;

    public static readonly ElasticMappingResolver NullInstance = new(() => null);

    public ElasticMappingResolver(Func<TypeMapping?> getMapping, Inferrer? inferrer = null, TimeProvider? timeProvider = null, ILogger? logger = null)
    {
        _inferrer = inferrer;
        _timeProvider = timeProvider ?? TimeProvider.System;
        _logger = logger ?? NullLogger.Instance;
        _inferredCodeProperties = new Lazy<Properties?>(() => InferCodeProperties(_codeMapping?.Properties), LazyThreadSafetyMode.ExecutionAndPublication);
        _cache = new MappingCache(getMapping, serverMapping => MergeProperties(_inferredCodeProperties.Value, serverMapping?.Properties), _timeProvider, _logger);
    }

    public ElasticMappingResolver(TypeMapping codeMapping, Inferrer inferrer, Func<TypeMapping?> getMapping, TimeProvider? timeProvider = null, ILogger? logger = null)
        : this(getMapping, inferrer, timeProvider, logger)
    {
        _codeMapping = codeMapping;
    }

    /// <summary>
    /// Maximum age of the loaded server mapping before an ordinary resolution will reload it. This also acts
    /// as the ceiling when backing off repeated reloads triggered by fields that cannot be resolved.
    /// </summary>
    /// <exception cref="ArgumentOutOfRangeException">The value is negative.</exception>
    public TimeSpan MappingRefreshInterval
    {
        get => _cache.RefreshInterval;
        set
        {
            ArgumentOutOfRangeException.ThrowIfLessThan(value, TimeSpan.Zero);
            _cache.RefreshInterval = value;
        }
    }

    /// <summary>
    /// Minimum interval between server mapping reloads that are triggered by a field which could not be
    /// resolved from the loaded mapping. A resolution failure is the strongest available signal that the
    /// index mapping changed (fields created by dynamic templates only exist after the first document that
    /// uses them is indexed), so this is intentionally much shorter than <see cref="MappingRefreshInterval"/>.
    /// Reloads that do not resolve the field back off exponentially up to <see cref="MappingRefreshInterval"/>.
    /// </summary>
    /// <remarks>
    /// A reload walks and merges the whole property tree, which on a large mapping costs several milliseconds
    /// and allocates megabytes, so this trades staleness against that cost rather than against network time.
    /// Set to <see cref="TimeSpan.Zero"/> to always reload on an unresolved field.
    /// </remarks>
    /// <exception cref="ArgumentOutOfRangeException">The value is negative.</exception>
    public TimeSpan UnmappedFieldRefreshInterval
    {
        get => _cache.UnmappedFieldRefreshInterval;
        set
        {
            ArgumentOutOfRangeException.ThrowIfLessThan(value, TimeSpan.Zero);
            _cache.UnmappedFieldRefreshInterval = value;
        }
    }

    /// <summary>
    /// Maximum time a resolution will wait for a server mapping reload that is already in flight. Only one
    /// reload runs at a time, so concurrent lookups of a field that is missing from the loaded mapping wait
    /// for that reload instead of issuing their own.
    /// </summary>
    /// <remarks>
    /// Waiting is never more expensive than performing the reload, so this must comfortably exceed the
    /// latency of the configured mapping fetch; giving up early resolves the field as unmapped even though a
    /// reload that could have resolved it was already running. It is bounded rather than infinite so an
    /// unresponsive cluster cannot pin request threads. Use <see cref="Timeout.InfiniteTimeSpan"/> to wait
    /// indefinitely, which is safe when the fetch callback has its own timeout (the Elasticsearch client
    /// applies one by default).
    /// </remarks>
    /// <exception cref="ArgumentOutOfRangeException">
    /// The value is negative or zero and is not <see cref="Timeout.InfiniteTimeSpan"/>.
    /// </exception>
    public TimeSpan MappingRefreshWaitTimeout
    {
        get => _cache.RefreshWaitTimeout;
        set
        {
            if (value != Timeout.InfiniteTimeSpan)
                ArgumentOutOfRangeException.ThrowIfLessThanOrEqual(value, TimeSpan.Zero);

            _cache.RefreshWaitTimeout = value;
        }
    }

    /// <summary>
    /// Approximate upper bound on the number of resolved field mappings held in memory. Field names come from
    /// user supplied queries, so this bounds memory usage when queries reference many distinct (often
    /// non-existent) fields.
    /// </summary>
    /// <remarks>
    /// When the bound is reached, cached misses are evicted first so that an abusive caller sending many
    /// unknown field names cannot displace the resolutions real queries depend on. Set to zero or less to
    /// disable caching entirely.
    /// </remarks>
    public int MaxCachedFields
    {
        get => _cache.MaxCachedFields;
        set => _cache.MaxCachedFields = value;
    }

    /// <summary>
    /// Approximate number of field names currently held in the resolved mapping cache.
    /// </summary>
    public long CachedFieldCount => _cache.CachedFieldCount;


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
        _cache.Reset();
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

        _cache.InvalidateField(field!);
        _logger.LogTrace("Invalidated field mapping: {Field}", field);
    }

    public FieldMapping? GetMapping(string? field, bool followAlias = false)
    {
        if (String.IsNullOrWhiteSpace(field))
            return null;

        if (!_cache.HasServerMappingFunc && _codeMapping is null)
            throw new InvalidOperationException("No mappings are available.");

        var snapshot = _cache.Current;

        if (_cache.TryGetField(field!, snapshot, out var cached))
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
            if (_cache.Refresh(triggeredByUnmappedField: true) != MappingRefreshResult.Updated)
            {
                if (_logger.IsEnabled(LogLevel.Trace))
                    _logger.LogTrace("Cached mapping (not found): {Field}=<null>", field);

                return cached;
            }

            return ResolveMapping(field!, followAlias, _cache.Current, reloadedForUnmappedField: true);
        }

        return ResolveMapping(field!, followAlias, snapshot, reloadedForUnmappedField: false);
    }

    private FieldMapping ResolveMapping(string field, bool followAlias, MappingSnapshot snapshot, bool reloadedForUnmappedField)
    {
        var lastRefreshResult = MappingRefreshResult.Skipped;
        bool reloaded = reloadedForUnmappedField;
        bool reloadedForMiss = reloadedForUnmappedField;

        // Load the server mapping the first time one is needed. This deliberately does not arm the
        // unmapped field throttle: a cold start fetch must never suppress the first miss driven reload,
        // otherwise fields created after startup resolve as unmapped until the throttle expires.
        if (!snapshot.Fetched && !reloaded)
        {
            lastRefreshResult = _cache.Refresh(triggeredByUnmappedField: false);
            if (lastRefreshResult == MappingRefreshResult.Updated)
            {
                snapshot = _cache.Current;
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
                    lastRefreshResult = _cache.Refresh(triggeredByUnmappedField: true);
                    if (lastRefreshResult == MappingRefreshResult.Updated)
                    {
                        reloaded = true;
                        reloadedForMiss = true;
                        depth = -1;
                        resolvedFieldName.Clear();
                        snapshot = _cache.Current;
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
                _cache.CacheField(field, resolvedMapping, snapshot.Version);

                // A miss driven reload that resolved the field is proof the mapping really had changed:
                // return to the fast base interval so the next schema change is picked up quickly.
                if (reloadedForMiss)
                    _cache.ResetUnmappedRefreshBackoff();

                if (_logger.IsEnabled(LogLevel.Trace))
                    _logger.LogTrace("Resolved mapping: {Field}={FieldPath}:{FieldType}", field, resolvedMapping.FullPath, resolvedMapping.Property?.Type);

                if (followAlias && resolvedMapping.Property is FieldAliasProperty fieldAlias)
                    return GetMapping(fieldAlias.Path?.Name) ?? resolvedMapping;

                return resolvedMapping;
            }

            currentProperties = GetChildProperties(fieldMapping);

            if (currentProperties is null)
                break;
        }

        // A freshly reloaded mapping that still does not contain the field means the field probably does not
        // exist at all (a typo or a query against a field that was never indexed). Back off so a flood of
        // bogus field names cannot turn every query into a mapping fetch.
        if (reloadedForMiss)
            _cache.RecordUnresolvedAfterRefresh(snapshot.Version);
        else if (lastRefreshResult == MappingRefreshResult.WaitTimedOut && _cache.ShouldLogSuppressedRefresh())
            _logger.LogWarning("Unable to resolve mapping for field {Field}. A server mapping reload was already in flight but did not complete within {WaitTimeout}, so this field is being treated as unmapped. Increase {Property} if the mapping fetch is expected to take longer than this", field,
                MappingRefreshWaitTimeout, nameof(MappingRefreshWaitTimeout));
        else if (lastRefreshResult == MappingRefreshResult.Throttled && snapshot.HasServerMapping && _cache.ShouldLogSuppressedRefresh())
            _logger.LogWarning("Unable to resolve mapping for field {Field}. The loaded server mapping is {MappingAge} old and a reload was suppressed by the {RefreshInterval} unmapped field refresh throttle, so this field is being treated as unmapped", field,
                _timeProvider.GetUtcNow().UtcDateTime - snapshot.CreatedUtc, _cache.CurrentUnmappedRefreshInterval);

        if (_logger.IsEnabled(LogLevel.Trace))
            _logger.LogTrace("Mapping not found: {Field}", field);

        // A cached miss is always revalidated against an in-flight or throttled reload before it is trusted
        // (see GetMapping), so caching this result cannot pin a stale answer.
        var notFoundMapping = new FieldMapping(resolvedFieldName.ToString(), null, snapshot.CreatedUtc, snapshot.Version);
        _cache.CacheField(field, notFoundMapping, snapshot.Version);

        return notFoundMapping;
    }

    private string? ResolvePropertyName(PropertyName? key)
    {
        if (key?.Name is null)
            return null;

        return _inferrer is not null ? _inferrer.PropertyName(key) : key.Name;
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
        var fields = GetChildProperties(multiFieldProperty);
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

    /// <summary>
    /// Returns the child properties a field name can descend into. Object and nested properties hold
    /// sub-objects in <c>Properties</c>; every other property type can only hold multi-fields. Using one
    /// accessor for every property type keeps resolution and merging symmetric, so a multi-field on a
    /// keyword, date or numeric property behaves exactly like one on a text property.
    /// </summary>
    private static Properties? GetOwnChildProperties(IProperty property)
    {
        return property switch
        {
            ObjectProperty objectProperty => objectProperty.Properties ?? objectProperty.Fields,
            NestedProperty nestedProperty => nestedProperty.Properties ?? nestedProperty.Fields,
            _ => property.GetFields()
        };
    }

    /// <summary>
    /// Returns the child properties of a property, preferring the merged view recorded while combining the
    /// code mapping with the server mapping.
    /// </summary>
    private Properties? GetChildProperties(IProperty property)
    {
        // Merged children only exist when a code mapping was supplied, so resolvers backed purely by the
        // server mapping never pay for the lookup.
        if (_codeMapping is not null && _mergedChildProperties.TryGetValue(property, out var mergedChildren))
            return mergedChildren;

        return GetOwnChildProperties(property);
    }

    /// <summary>
    /// Combines the properties declared in code with the properties reported by the server, preferring the
    /// server definition and layering code-only properties on top.
    /// </summary>
    /// <remarks>
    /// Merging never mutates the server mapping. Where a property is declared in both mappings its combined
    /// children are recorded in a side table keyed by the server property instance, which keeps the mapping
    /// returned by the <c>getMapping</c> callback safe to share and makes merging work identically for every
    /// property type rather than only the handful that expose settable sub-property collections.
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

                var codeChildren = GetOwnChildProperties(codeProperty);
                if (codeChildren is not null)
                {
                    var serverChildren = GetOwnChildProperties(merged);
                    var mergedChildren = MergeCodeAndServerProperties(codeChildren, serverChildren);
                    if (mergedChildren is not null && !ReferenceEquals(mergedChildren, serverChildren))
                        _mergedChildProperties.AddOrUpdate(merged, mergedChildren);
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
        _cache.Dispose();
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
