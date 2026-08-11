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
    private static readonly TimeSpan _suppressedRefreshWarningInterval = TimeSpan.FromMinutes(1);

    private readonly TypeMapping? _codeMapping;
    private readonly Inferrer? _inferrer;
    private readonly MappingCache _cache;
    private readonly ConditionalWeakTable<IProperty, ConcurrentDictionary<string, object>> _propertyMetadata = new();
    private readonly TimeProvider _timeProvider;
    private readonly ILogger _logger;
    private long _lastSuppressedRefreshWarningTimestamp;

    public static readonly ElasticMappingResolver NullInstance = new(() => null);

    public ElasticMappingResolver(Func<TypeMapping?> getMapping, Inferrer? inferrer = null, TimeProvider? timeProvider = null, ILogger? logger = null)
    {
        _inferrer = inferrer;
        _timeProvider = timeProvider ?? TimeProvider.System;
        _logger = logger ?? NullLogger.Instance;
        _cache = new MappingCache(getMapping, BuildMergedProperties, _timeProvider, _logger);
    }

    public ElasticMappingResolver(TypeMapping codeMapping, Inferrer inferrer, Func<TypeMapping?> getMapping, TimeProvider? timeProvider = null, ILogger? logger = null)
        : this(getMapping, inferrer, timeProvider, logger)
    {
        _codeMapping = codeMapping;
    }

    /// <summary>
    /// Minimum interval between server mapping reloads that are triggered by a field which could not be
    /// resolved from the loaded mapping. A resolution failure is the strongest available signal that the
    /// index mapping changed (fields created by dynamic templates only exist after the first document that
    /// uses them is indexed).
    /// </summary>
    /// <remarks>
    /// A reload walks and merges the whole property tree, which on a large mapping costs several milliseconds
    /// and allocates megabytes, so this trades staleness against that cost rather than against network time.
    /// </remarks>
    /// <exception cref="ArgumentOutOfRangeException">The value is negative or zero.</exception>
    public TimeSpan UnmappedFieldRefreshInterval
    {
        get => _cache.UnmappedFieldRefreshInterval;
        set
        {
            ArgumentOutOfRangeException.ThrowIfLessThanOrEqual(value, TimeSpan.Zero);
            _cache.UnmappedFieldRefreshInterval = value;
        }
    }

    /// <summary>
    /// Maximum time a resolution will wait for a server mapping reload that is already in flight. Only one
    /// reload runs at a time, so concurrent lookups of a field that is missing from the loaded mapping wait
    /// for that reload instead of issuing their own.
    /// </summary>
    /// <remarks>
    /// This must comfortably exceed the latency of the configured mapping fetch; giving up early resolves
    /// the field as unmapped even though a reload that could have resolved it was already running. It is
    /// bounded so an unresponsive cluster cannot pin request threads indefinitely. The mapping callback must
    /// enforce a shorter timeout and must not call back into this resolver.
    /// </remarks>
    /// <exception cref="ArgumentOutOfRangeException">The value is negative or zero, or exceeds the maximum wait a semaphore accepts.</exception>
    public TimeSpan MappingRefreshWaitTimeout
    {
        get => _cache.RefreshWaitTimeout;
        set
        {
            ArgumentOutOfRangeException.ThrowIfLessThanOrEqual(value, TimeSpan.Zero);
            ArgumentOutOfRangeException.ThrowIfGreaterThan(value, TimeSpan.FromMilliseconds(Int32.MaxValue));
            _cache.RefreshWaitTimeout = value;
        }
    }

    /// <summary>
    /// Clears the cached mapping, forcing a fresh fetch from the server on the next access.
    /// </summary>
    /// <remarks>
    /// Unresolved fields automatically reload the server mapping at most once per
    /// <see cref="UnmappedFieldRefreshInterval"/>. This method bypasses that throttle and discards the
    /// current mapping snapshot so the next resolution fetches it again.
    /// </remarks>
    public void RefreshMapping()
    {
        _cache.Reset();
        _logger.LogInformation("Mapping refresh triggered");
    }

    public FieldMapping? GetMapping(string? field, bool followAlias = false)
    {
        if (String.IsNullOrWhiteSpace(field))
            return null;

        if (!_cache.HasServerMappingFunc && _codeMapping is null)
            throw new InvalidOperationException("No mappings are available.");

        var snapshot = _cache.Current;

        if (snapshot.TryGetField(field!, out var cached))
        {
            if (_logger.IsEnabled(LogLevel.Trace))
                _logger.LogTrace("Cached mapping: {Field}={FieldPath}:{FieldType}", field, cached.FullPath, cached.Property?.Type);

            return FollowAlias(cached, followAlias);
        }

        return ResolveMapping(field!, followAlias, snapshot);
    }

    private FieldMapping? ResolveMapping(string field, bool followAlias, MappingSnapshot snapshot)
    {
        var loadResult = MappingRefreshResult.Unavailable;
        bool hadFetchedSnapshot = snapshot.Fetched;

        if (!hadFetchedSnapshot)
        {
            loadResult = _cache.LoadInitial(snapshot);
            snapshot = _cache.Current;
        }

        var resolved = Resolve(field, snapshot);

        // The field is unknown to the loaded mapping, which is the strongest available signal that the mapping
        // changed, so reload once and resolve again against the new one. Do not reload immediately after this
        // resolution already loaded or joined the initial mapping; that would repeat the same work and can
        // consume the join timeout twice.
        if (!resolved.Found && hadFetchedSnapshot)
        {
            loadResult = _cache.ReloadForMissingField(snapshot);
            if (loadResult == MappingRefreshResult.Updated)
            {
                snapshot = _cache.Current;
                resolved = Resolve(field, snapshot);
            }
        }

        snapshot.CacheField(resolved);

        if (resolved.Found)
        {
            if (_logger.IsEnabled(LogLevel.Trace))
                _logger.LogTrace("Resolved mapping: {Field}={FieldPath}:{FieldType}", field, resolved.FullPath, resolved.Property?.Type);

            return FollowAlias(resolved, followAlias);
        }

        LogUnresolvedField(field, loadResult, snapshot);
        return resolved;
    }

    private FieldMapping? FollowAlias(FieldMapping mapping, bool followAlias)
    {
        if (!followAlias || mapping.Property is not FieldAliasProperty alias)
            return mapping;

        return GetMapping(alias.Path?.Name);
    }

    /// <summary>
    /// Walks a dotted field name through the merged property tree, resolving each part to its canonical
    /// mapping name. Parts past the deepest resolvable one are appended unchanged so callers still get a
    /// usable field path for an unmapped field.
    /// </summary>
    private static FieldMapping Resolve(string field, MappingSnapshot snapshot)
    {
        var properties = snapshot.Properties;
        int start = 0;
        StringBuilder? resolvedName = null;
        MergedNode? node = null;

        while (true)
        {
            int separator = field.IndexOf('.', start);
            string part = separator < 0 ? field[start..] : field[start..separator];

            if (properties is null || !properties.TryGetNode(part, out var matched))
            {
                // Unresolvable from here down: keep what was resolved and append the remainder verbatim.
                string remainder = field[start..];
                if (resolvedName is null)
                    return new FieldMapping(remainder, null);

                return new FieldMapping(resolvedName.Append('.').Append(remainder).ToString(), null);
            }

            node = matched;

            if (resolvedName is null)
                resolvedName = new StringBuilder(matched.Name);
            else
                resolvedName.Append('.').Append(matched.Name);

            if (separator < 0)
                return new FieldMapping(resolvedName.ToString(), node.Property, node.Children);

            properties = matched.Children;
            start = separator + 1;
        }
    }

    private void LogUnresolvedField(string field, MappingRefreshResult loadResult, MappingSnapshot snapshot)
    {
        if (loadResult == MappingRefreshResult.WaitTimedOut && ShouldLogSuppressedRefresh())
        {
            _logger.LogWarning("Unable to resolve mapping for field {Field}. A server mapping reload was already in flight but did not complete within {WaitTimeout}, so this field is being treated as unmapped. Increase {Property} if the mapping fetch is expected to take longer than this",
                field, MappingRefreshWaitTimeout, nameof(MappingRefreshWaitTimeout));
        }
        else if (loadResult == MappingRefreshResult.Throttled && snapshot.HasServerMapping && ShouldLogSuppressedRefresh())
        {
            _logger.LogWarning("Unable to resolve mapping for field {Field}. The loaded server mapping is {MappingAge} old and a reload was suppressed by the {RefreshInterval} unmapped field refresh throttle, so this field is being treated as unmapped",
                field, _timeProvider.GetUtcNow().UtcDateTime - snapshot.CreatedUtc, UnmappedFieldRefreshInterval);
        }
        else if (_logger.IsEnabled(LogLevel.Trace))
        {
            _logger.LogTrace("Mapping not found: {Field}", field);
        }
    }

    /// <summary>
    /// Rate limits warnings about suppressed reloads so a flood of queries against non-existent fields cannot
    /// flood the log.
    /// </summary>
    private bool ShouldLogSuppressedRefresh()
    {
        if (!_logger.IsEnabled(LogLevel.Warning))
            return false;

        long last = Interlocked.Read(ref _lastSuppressedRefreshWarningTimestamp);
        if (last != 0 && _timeProvider.GetElapsedTime(last) < _suppressedRefreshWarningInterval)
            return false;

        long timestamp = _timeProvider.GetTimestamp();
        return Interlocked.CompareExchange(ref _lastSuppressedRefreshWarningTimestamp, timestamp == 0 ? 1 : timestamp, last) == last;
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

        var children = mapping.Children;
        if (children is null || children.Count == 0)
            return mapping.FullPath;

        var preferred = children.Nodes.FirstOrDefault(n => n.Name == preferredSubField && IsNonAnalyzed(n.Property));
        var nonAnalyzed = preferred ?? children.Nodes.FirstOrDefault(n => IsNonAnalyzed(n.Property));

        return nonAnalyzed is not null ? mapping.FullPath + "." + nonAnalyzed.Name : mapping.FullPath;
    }

    private bool IsNonAnalyzed(IProperty property) => property is KeywordProperty || !IsPropertyAnalyzed(property);

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

    /// <summary>
    /// Builds the merged view of the code and server mappings as a name-keyed tree, ready for resolution.
    /// </summary>
    /// <remarks>
    /// Pure with respect to both mappings: nothing is written back into the mapping the <c>getMapping</c>
    /// callback returned, so that instance stays safe to cache and share. Keying children by name rather than
    /// by <see cref="IProperty"/> instance is what makes merging work for every property type instead of only
    /// the few that expose settable sub-property collections, and is also why reusing one property instance
    /// across several fields cannot leak one field's sub-fields into another's.
    /// </remarks>
    private MergedProperties? BuildMergedProperties(TypeMapping? serverMapping)
    {
        return Merge(_codeMapping?.Properties, serverMapping?.Properties);
    }

    private MergedProperties? Merge(Properties? codeProperties, Properties? serverProperties)
    {
        if (codeProperties is null && serverProperties is null)
            return null;

        var nodes = new List<MergedNode>();
        var codeByName = IndexCodeProperties(codeProperties);
        var seen = new HashSet<string>(StringComparer.Ordinal);

        if (serverProperties is not null)
        {
            foreach (var kvp in serverProperties)
            {
                string? name = ResolvePropertyName(kvp.Key);
                if (name is null || !seen.Add(name))
                    continue;

                // The server mapping is authoritative, so a code property only contributes children when both
                // sides agree on whether the field is a container.
                codeByName.TryGetValue(name, out var codeProperty);
                var codeChildren = codeProperty is not null && IsContainer(codeProperty) == IsContainer(kvp.Value)
                    ? GetChildProperties(codeProperty)
                    : null;

                if (codeProperty is not null)
                    CopyPropertyMetadata(codeProperty, kvp.Value);

                nodes.Add(new MergedNode(name, kvp.Value, Merge(codeChildren, GetChildProperties(kvp.Value))));
            }
        }

        foreach (var (name, property) in codeByName)
        {
            if (!seen.Add(name))
                continue;

            nodes.Add(new MergedNode(name, property, Merge(GetChildProperties(property), null)));
        }

        return nodes.Count > 0 ? new MergedProperties(nodes) : null;
    }

    /// <summary>
    /// Resolves code mapping property names and alias paths through the inferrer, so a mapping declared with
    /// property expressions is keyed by the same names the server reports.
    /// </summary>
    private Dictionary<string, IProperty> IndexCodeProperties(Properties? codeProperties)
    {
        var indexed = new Dictionary<string, IProperty>(StringComparer.Ordinal);
        if (codeProperties is null)
            return indexed;

        foreach (var kvp in codeProperties)
        {
            string? name = ResolvePropertyName(kvp.Key);
            if (name is null)
                continue;

            var property = kvp.Value;
            if (_inferrer is not null && property is FieldAliasProperty alias)
            {
                var resolvedAlias = new FieldAliasProperty { Path = _inferrer.Field(alias.Path!) ?? alias.Path };
                CopyPropertyMetadata(alias, resolvedAlias);
                property = resolvedAlias;
            }

            indexed[name] = property;
        }

        return indexed;
    }

    private string? ResolvePropertyName(PropertyName? key)
    {
        if (key is null)
            return null;

        // A property expression has no literal name until the inferrer resolves it.
        if (_inferrer is not null)
            return _inferrer.PropertyName(key);

        return key.Name;
    }

    private static bool IsContainer(IProperty property) => property is ObjectProperty or NestedProperty;

    /// <summary>
    /// Returns the child properties a field name can descend into. Object and nested properties hold
    /// sub-objects in <c>Properties</c>; every other property type can only hold multi-fields. Using one
    /// accessor for every property type is what makes a multi-field on a keyword, date or numeric property
    /// behave exactly like one on a text property.
    /// </summary>
    private static Properties? GetChildProperties(IProperty property)
    {
        return property switch
        {
            ObjectProperty objectProperty => objectProperty.Properties,
            NestedProperty nestedProperty => nestedProperty.Properties,
            _ => property.GetFields()
        };
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
        : this(path, property, (MergedProperties?)null)
    {
    }

    internal FieldMapping(string path, IProperty? property)
        : this(path, property, (MergedProperties?)null)
    {
    }

    internal FieldMapping(string path, IProperty? property, MergedProperties? children)
    {
        FullPath = path;
        Property = property;
        Children = children;
    }

    public bool Found => Property is not null;
    public string FullPath { get; private set; }
    public IProperty? Property { get; private set; }
    public DateTime Date { get; private set; } = DateTime.UtcNow;

    /// <summary>
    /// Merged sub-objects and multi-fields of this field, captured during resolution so sub-field lookups do
    /// not have to walk the mapping again.
    /// </summary>
    internal MergedProperties? Children { get; }
}

/// <summary>A name-indexed merged view of the code and server properties at one mapping level.</summary>
internal sealed class MergedProperties
{
    private readonly IReadOnlyList<MergedNode> _nodes;
    private readonly Dictionary<string, MergedNode> _byExactName;
    private readonly Dictionary<string, MergedNode> _byIgnoreCaseName;

    public MergedProperties(IReadOnlyList<MergedNode> nodes)
    {
        _nodes = nodes;
        _byExactName = new Dictionary<string, MergedNode>(nodes.Count, StringComparer.Ordinal);
        _byIgnoreCaseName = new Dictionary<string, MergedNode>(nodes.Count, StringComparer.OrdinalIgnoreCase);

        foreach (var node in nodes)
        {
            _byExactName[node.Name] = node;
            _byIgnoreCaseName.TryAdd(node.Name, node);
        }
    }

    public int Count => _nodes.Count;

    public IReadOnlyList<MergedNode> Nodes => _nodes;

    public bool TryGetNode(string name, out MergedNode node)
    {
        return _byExactName.TryGetValue(name, out node!) || _byIgnoreCaseName.TryGetValue(name, out node!);
    }
}

/// <summary>A canonical property and the merged children reachable through its field name.</summary>
internal sealed class MergedNode
{
    public MergedNode(string name, IProperty property, MergedProperties? children)
    {
        Name = name;
        Property = property;
        Children = children;
    }

    public string Name { get; }

    public IProperty Property { get; }

    public MergedProperties? Children { get; }
}
