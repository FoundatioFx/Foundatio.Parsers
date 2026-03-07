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
using Exceptionless.DateTimeExtensions;
using Foundatio.Parsers.ElasticQueries.Extensions;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Logging.Abstractions;

namespace Foundatio.Parsers.ElasticQueries;

public class ElasticMappingResolver : IDisposable
{
    private TypeMapping _serverMapping;
    private readonly TypeMapping _codeMapping;
    private readonly Inferrer _inferrer;
    private readonly ConcurrentDictionary<string, FieldMapping> _mappingCache = new();
    private readonly object _mappingLock = new();
    private readonly SemaphoreSlim _fetchSemaphore = new(1, 1);
    private long _refreshEpoch;
    private readonly TimeProvider _timeProvider;
    private readonly ConditionalWeakTable<IProperty, ConcurrentDictionary<string, object>> _propertyMetadata = new();
    private readonly ILogger _logger;

    public static readonly ElasticMappingResolver NullInstance = new(() => null);

    public ElasticMappingResolver(Func<TypeMapping> getMapping, Inferrer inferrer = null, TimeProvider timeProvider = null, ILogger logger = null)
    {
        GetServerMappingFunc = getMapping;
        _inferrer = inferrer;
        _timeProvider = timeProvider ?? TimeProvider.System;
        _logger = logger ?? NullLogger.Instance;
    }

    public ElasticMappingResolver(TypeMapping codeMapping, Inferrer inferrer, Func<TypeMapping> getMapping, TimeProvider timeProvider = null, ILogger logger = null)
        : this(getMapping, inferrer, timeProvider, logger)
    {
        _codeMapping = codeMapping;
    }

    /// <summary>
    /// Clears the cached mapping, forcing a fresh fetch from the server on the next access.
    /// </summary>
    /// <remarks>
    /// Mappings are automatically refreshed at most once per minute. This method bypasses that
    /// throttle and is primarily useful in unit tests where index mappings change rapidly.
    /// In production, the automatic refresh is typically sufficient.
    /// </remarks>
    public void RefreshMapping()
    {
        lock (_mappingLock)
        {
            Interlocked.Increment(ref _refreshEpoch);
            _serverMapping = null;
            Interlocked.Exchange(ref _lastMappingUpdateTicks, 0);
            _mappingCache.Clear();
        }

        _logger.LogInformation("Mapping refresh triggered");
    }

    public FieldMapping GetMapping(string field, bool followAlias = false)
    {
        if (String.IsNullOrWhiteSpace(field))
            return null;

        if (GetServerMappingFunc == null && _codeMapping == null)
            throw new InvalidOperationException("No mappings are available.");

        long currentEpoch = Interlocked.Read(ref _refreshEpoch);

        if (_mappingCache.TryGetValue(field, out var mapping) && mapping.Epoch >= currentEpoch)
        {
            long lastUpdateTicks = Interlocked.Read(ref _lastMappingUpdateTicks);
            bool mappingCurrent = lastUpdateTicks == 0
                || (mapping.ServerMapTime.HasValue && mapping.ServerMapTime.Value.Ticks >= lastUpdateTicks);

            if (mapping.Found && mappingCurrent)
            {
                if (followAlias && mapping.Property is FieldAliasProperty fieldAlias)
                {
                    _logger.LogTrace("Cached alias mapping: {Field}={FieldPath}:{FieldType}", field, mapping.FullPath, mapping.Property.Type);
                    return GetMapping(fieldAlias.Path.Name);
                }

                _logger.LogTrace("Cached mapping: {Field}={FieldPath}:{FieldType}", field, mapping.FullPath, mapping.Property?.Type);
                return mapping;
            }

            if (!mapping.Found && mappingCurrent && !GetServerMapping())
            {
                _logger.LogTrace("Cached mapping (not found): {Field}=<null>", field);
                return mapping;
            }
        }

        string[] fieldParts = field.Split('.');
        var resolvedFieldName = new StringBuilder();

        TypeMapping serverMapping;
        DateTime? mappingServerTime;
        lock (_mappingLock)
        {
            serverMapping = _serverMapping;
            long ticks = Interlocked.Read(ref _lastMappingUpdateTicks);
            mappingServerTime = ticks > 0 ? new DateTime(ticks, DateTimeKind.Utc) : null;
        }

        if (serverMapping == null && GetServerMappingFunc != null && GetServerMapping())
        {
            lock (_mappingLock)
            {
                serverMapping = _serverMapping;
                long ticks = Interlocked.Read(ref _lastMappingUpdateTicks);
                mappingServerTime = ticks > 0 ? new DateTime(ticks, DateTimeKind.Utc) : null;
            }
        }

        var currentProperties = MergeProperties(_codeMapping?.Properties, serverMapping?.Properties);

        for (int depth = 0; depth < fieldParts.Length; depth++)
        {
            string fieldPart = fieldParts[depth];
            IProperty fieldMapping = null;
            PropertyName foundPropertyName = null;
            if (currentProperties is null || !currentProperties.TryGetProperty(fieldPart, out fieldMapping))
            {
                // check to see if there is a name match by iterating through the dictionary keys
                if (currentProperties is not null)
                {
                    foreach (var kvp in (IDictionary<PropertyName, IProperty>)currentProperties)
                    {
                        string propertyName = null;
                        if (_inferrer is not null && kvp.Key?.Name is not null)
                            propertyName = _inferrer.PropertyName(kvp.Key);
                        else if (kvp.Key?.Name is not null)
                            propertyName = kvp.Key.Name;

                        if (propertyName is not null && propertyName.Equals(fieldPart, StringComparison.OrdinalIgnoreCase))
                        {
                            fieldMapping = kvp.Value;
                            foundPropertyName = kvp.Key;
                            break;
                        }
                    }
                }

                // no mapping found, call GetServerMapping again in case it hasn't been called recently and there are possibly new mappings
                if (fieldMapping is null && GetServerMapping())
                {
                    depth = -1;
                    resolvedFieldName.Clear();
                    lock (_mappingLock)
                    {
                        serverMapping = _serverMapping;
                        long ticks = Interlocked.Read(ref _lastMappingUpdateTicks);
                        mappingServerTime = ticks > 0 ? new DateTime(ticks, DateTimeKind.Utc) : null;
                    }
                    currentProperties = MergeProperties(_codeMapping?.Properties, serverMapping?.Properties);
                    continue;
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
            else
            {
                // TryGetProperty succeeded, find the PropertyName key for this mapping
                foundPropertyName = ((IDictionary<PropertyName, IProperty>)currentProperties)
                    .FirstOrDefault(kvp => kvp.Value == fieldMapping).Key;
            }

            // Determine the property name - use foundPropertyName if available, otherwise fall back to fieldPart
            string resolvedName;
            if (foundPropertyName is not null && _inferrer is not null && foundPropertyName.Name is not null)
                resolvedName = _inferrer.PropertyName(foundPropertyName);
            else if (foundPropertyName is not null && foundPropertyName.Name is not null)
                resolvedName = foundPropertyName.Name;
            else
                resolvedName = fieldPart;

            if (depth > 0)
                resolvedFieldName.Append('.');
            resolvedFieldName.Append(resolvedName);

            if (depth == fieldParts.Length - 1)
            {
                var resolvedMapping = new FieldMapping(resolvedFieldName.ToString(), fieldMapping, mappingServerTime, currentEpoch);
                if (IsSnapshotCurrent(currentEpoch, mappingServerTime))
                    _mappingCache.AddOrUpdate(field, resolvedMapping, (_, existing) =>
                        existing.Epoch > resolvedMapping.Epoch ? existing : resolvedMapping);
                _logger.LogTrace("Resolved mapping: {Field}={FieldPath}:{FieldType}", field, resolvedMapping.FullPath, resolvedMapping.Property?.Type);

                if (followAlias && resolvedMapping.Property is FieldAliasProperty fieldAlias)
                    return GetMapping(fieldAlias.Path.Name);

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

        _logger.LogTrace("Mapping not found: {field}", field);
        var notFoundMapping = new FieldMapping(resolvedFieldName.ToString(), null, mappingServerTime, currentEpoch);
        if (IsSnapshotCurrent(currentEpoch, mappingServerTime))
            _mappingCache.AddOrUpdate(field, notFoundMapping, (_, existing) =>
                existing.Epoch > notFoundMapping.Epoch ? existing : notFoundMapping);

        return notFoundMapping;
    }

    public FieldMapping GetMapping(Field field, bool followAlias = false)
    {
        if (_inferrer == null)
            throw new InvalidOperationException("Unable to resolve Field without inferrer");

        return GetMapping(_inferrer.Field(field), followAlias);
    }

    public IProperty GetMappingProperty(string field, bool followAlias = false)
    {
        return GetMapping(field, followAlias)?.Property;
    }

    public IProperty GetMappingProperty(Field field, bool followAlias = false)
    {
        return GetMapping(field, followAlias)?.Property;
    }

    public string GetResolvedField(string field)
    {
        var result = GetMapping(field, true);
        return result?.FullPath ?? field;
    }

    public string GetResolvedField(Field field)
    {
        if (_inferrer == null)
            throw new InvalidOperationException("Unable to resolve Field without inferrer");

        return GetResolvedField(_inferrer.Field(field));
    }

    public string GetSortFieldName(string field)
    {
        return GetNonAnalyzedFieldName(field, ElasticMapping.SortFieldName);
    }

    public string GetSortFieldName(Field field)
    {
        return GetNonAnalyzedFieldName(GetResolvedField(field), ElasticMapping.SortFieldName);
    }

    public string GetAggregationsFieldName(string field)
    {
        return GetNonAnalyzedFieldName(field, ElasticMapping.KeywordFieldName);
    }

    public string GetAggregationsFieldName(Field field)
    {
        return GetNonAnalyzedFieldName(field, ElasticMapping.KeywordFieldName);
    }

    public string GetNonAnalyzedFieldName(Field field, string preferredSubField = null)
    {
        return GetNonAnalyzedFieldName(GetResolvedField(field), preferredSubField);
    }

    public string GetNonAnalyzedFieldName(string field, string preferredSubField = null)
    {
        if (String.IsNullOrEmpty(field))
            return field;

        var mapping = GetMapping(field, true);

        if (mapping?.Property == null || !IsPropertyAnalyzed(mapping.Property))
            return field;

        var multiFieldProperty = mapping.Property;
        var fields = multiFieldProperty.GetFields();
        if ((IDictionary<PropertyName, IProperty>)fields is not { Count: > 0 })
            return mapping.FullPath;

        var nonAnalyzedProperty = fields.OrderByDescending(kvp => kvp.Key.Name == preferredSubField).FirstOrDefault(kvp =>
        {
            if (kvp.Value is KeywordProperty)
                return true;

            if (!IsPropertyAnalyzed(kvp.Value))
                return true;

            return false;
        });

        if (nonAnalyzedProperty.Value != null)
            return mapping.FullPath + "." + nonAnalyzedProperty.Key.Name;

        return mapping.FullPath;
    }

    public bool IsPropertyAnalyzed(string field)
    {
        // assume default is analyzed
        if (String.IsNullOrEmpty(field))
            return true;

        var property = GetMapping(field, true);
        if (!property.Found)
            return false;

        return IsPropertyAnalyzed(property.Property);
    }

    public bool IsPropertyAnalyzed(IProperty property)
    {
        if (property is TextProperty textProperty)
            return !textProperty.Index.HasValue || textProperty.Index.Value;

        return false;
    }

    public bool IsNestedPropertyType(string field)
    {
        if (String.IsNullOrEmpty(field))
            return false;

        return GetMappingProperty(field, true) is NestedProperty;
    }

    public bool IsGeoPropertyType(string field)
    {
        if (String.IsNullOrEmpty(field))
            return false;

        return GetMappingProperty(field, true) is GeoPointProperty;
    }

    public bool IsNumericPropertyType(string field)
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

    public bool IsBooleanPropertyType(string field)
    {
        if (String.IsNullOrEmpty(field))
            return false;

        return GetMappingProperty(field, true) is BooleanProperty;
    }

    public bool IsDatePropertyType(string field)
    {
        if (String.IsNullOrEmpty(field))
            return false;

        return GetMappingProperty(field, true) is DateProperty or DateNanosProperty;
    }

    public FieldType GetFieldType(string field)
    {
        if (String.IsNullOrWhiteSpace(field))
            return FieldType.None;

        var property = GetMappingProperty(field, true);

        if (property?.Type == null)
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
            "long" => FieldType.Long,
            "long_range" => FieldType.LongRange,
            "match_only_text" => FieldType.MatchOnlyText,
            "murmur3" => FieldType.Murmur3,
            "nested" => FieldType.Nested,
            "none" => FieldType.None,
            "object" => FieldType.Object,
            "passthrough" => FieldType.Passthrough,
            "percolator" => FieldType.Percolator,
            "rank_feature" => FieldType.RankFeature,
            "rank_features" => FieldType.RankFeatures,
            "scaled_float" => FieldType.ScaledFloat,
            "search_as_you_type" => FieldType.SearchAsYouType,
            "semantic_text" => FieldType.SemanticText,
            "shape" => FieldType.Shape,
            "short" => FieldType.Short,
            "sparse_vector" => FieldType.SparseVector,
            "text" => FieldType.Text,
            "token_count" => FieldType.TokenCount,
            "version" => FieldType.Version,
            _ => FieldType.None,
        };
    }

    private Properties MergeProperties(Properties codeProperties, Properties serverProperties)
    {
        if (codeProperties == null && serverProperties == null)
            return null;

        Properties mergedCodeProperties = null;
        // resolve code mapping property expressions using inferrer
        if (codeProperties != null)
        {
            mergedCodeProperties = new Properties();

            foreach (var kvp in codeProperties)
            {
                var propertyName = kvp.Key;
                if (_inferrer is not null && (String.IsNullOrEmpty(kvp.Key.Name) || kvp.Value is FieldAliasProperty))
                    propertyName = _inferrer.PropertyName(kvp.Key) ?? kvp.Key;

                mergedCodeProperties[propertyName] = kvp.Value;
            }

            if (_inferrer != null)
            {
                // resolve field alias
                foreach (var kvp in codeProperties)
                {
                    if (kvp.Value is not FieldAliasProperty aliasProperty)
                        continue;

                    var newAliasProperty = new FieldAliasProperty
                    {
                        Path = _inferrer?.Field(aliasProperty.Path) ?? aliasProperty.Path,
                    };
                    CopyPropertyMetadata(aliasProperty, newAliasProperty);
                    mergedCodeProperties[kvp.Key] = newAliasProperty;
                }
            }
        }

        // no need to merge
        if (mergedCodeProperties == null || serverProperties == null)
            return mergedCodeProperties ?? serverProperties;

        Properties properties = new Properties();
        foreach (var serverProperty in serverProperties)
        {
            var merged = serverProperty.Value;

            if (mergedCodeProperties.TryGetProperty(serverProperty.Key, out var codeProperty))
            {
                // Copy local metadata from code property to merged property
                CopyPropertyMetadata(codeProperty, merged);

                switch (merged)
                {
                    case ObjectProperty objectProperty:
                        var codeObjectProperty = codeProperty as ObjectProperty;
                        objectProperty.Properties =
                            MergeProperties(codeObjectProperty?.Properties, objectProperty.Properties);
                        break;
                    case TextProperty textProperty:
                        var codeTextProperty = codeProperty as TextProperty;
                        textProperty.Fields = MergeProperties(codeTextProperty?.Fields, textProperty.Fields);
                        break;
                }
            }

            properties.Add(serverProperty.Key, merged);
        }

        foreach (var codeProperty in mergedCodeProperties)
        {
            if (properties.TryGetProperty(codeProperty.Key, out _))
                continue;

            properties.Add(codeProperty.Key, codeProperty.Value);
        }

        return properties;
    }

    private Func<TypeMapping> GetServerMappingFunc { get; set; }
    private long _lastMappingUpdateTicks;

    private bool IsSnapshotCurrent(long snapshotEpoch, DateTime? snapshotServerTime)
    {
        if (Interlocked.Read(ref _refreshEpoch) != snapshotEpoch)
            return false;

        long currentTicks = Interlocked.Read(ref _lastMappingUpdateTicks);
        if (currentTicks == 0)
            return true;

        return snapshotServerTime.HasValue && snapshotServerTime.Value.Ticks >= currentTicks;
    }

    /// <returns>true if a new mapping was fetched and applied; false if throttled or unavailable.</returns>
    private bool GetServerMapping()
    {
        if (GetServerMappingFunc == null)
            return false;

        long epochBeforeFetch;
        lock (_mappingLock)
        {
            long lastTicks = Interlocked.Read(ref _lastMappingUpdateTicks);
            if (lastTicks > 0 && new DateTime(lastTicks, DateTimeKind.Utc) > _timeProvider.GetUtcNow().UtcDateTime.SubtractMinutes(1))
                return false;
            epochBeforeFetch = Interlocked.Read(ref _refreshEpoch);
        }

        if (!_fetchSemaphore.Wait(0))
            return false;

        try
        {
            lock (_mappingLock)
            {
                long lastTicks = Interlocked.Read(ref _lastMappingUpdateTicks);
                if (lastTicks > 0 && new DateTime(lastTicks, DateTimeKind.Utc) > _timeProvider.GetUtcNow().UtcDateTime.SubtractMinutes(1))
                    return false;
            }

            TypeMapping newMapping;
            try
            {
                newMapping = GetServerMappingFunc();
            }
            catch (Exception ex) when (ex is not OutOfMemoryException and not StackOverflowException)
            {
                _logger.LogError(ex, "Error getting server mapping: {Message}", ex.Message);
                return false;
            }

            lock (_mappingLock)
            {
                if (Interlocked.Read(ref _refreshEpoch) != epochBeforeFetch)
                    return false;

                _serverMapping = newMapping;
                Interlocked.Exchange(ref _lastMappingUpdateTicks, _timeProvider.GetUtcNow().UtcDateTime.Ticks);
                _mappingCache.Clear();
            }

            _logger.LogInformation("Got server mapping");
            return true;
        }
        finally
        {
            _fetchSemaphore.Release();
        }
    }

    public static ElasticMappingResolver Create<T>(Action<TypeMappingDescriptor<T>> mappingBuilder, ElasticsearchClient client, ILogger logger = null) where T : class
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

    public static ElasticMappingResolver Create<T>(Action<TypeMappingDescriptor<T>> mappingBuilder, ElasticsearchClient client, string index, ILogger logger = null) where T : class
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

    public static ElasticMappingResolver Create<T>(Action<TypeMappingDescriptor<T>> mappingBuilder, Inferrer inferrer, Func<TypeMapping> getMapping, ILogger logger = null) where T : class
    {
        var descriptor = new TypeMappingDescriptor<T>();
        mappingBuilder(descriptor);
        return new ElasticMappingResolver(descriptor, inferrer, getMapping, logger: logger);
    }

    public static ElasticMappingResolver Create<T>(ElasticsearchClient client, ILogger logger = null)
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

    public static ElasticMappingResolver Create(ElasticsearchClient client, string index, ILogger logger = null)
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

    public static ElasticMappingResolver Create(Func<TypeMapping> getMapping, Inferrer inferrer, ILogger logger = null)
    {
        return new ElasticMappingResolver(getMapping, inferrer, logger: logger);
    }


    public IDictionary<string, object> GetPropertyMetadata(IProperty property)
    {
        if (property is null)
            return null;

        return _propertyMetadata.GetOrCreateValue(property);
    }

    public T GetPropertyMetadataValue<T>(IProperty property, string key, T defaultValue = default)
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
        _fetchSemaphore.Dispose();
    }
}

public class FieldMapping
{
    public FieldMapping(string path, IProperty property, DateTime? serverMapTime, long epoch = 0)
    {
        FullPath = path;
        Property = property;
        ServerMapTime = serverMapTime;
        Epoch = epoch;
    }

    public bool Found => Property != null;
    public string FullPath { get; private set; }
    public IProperty Property { get; private set; }
    public DateTime Date { get; private set; } = DateTime.UtcNow;
    internal DateTime? ServerMapTime { get; private set; }
    internal long Epoch { get; private set; }
}
