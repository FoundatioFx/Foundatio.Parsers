using System;
using System.Collections.Concurrent;
using System.Linq;
using System.Threading;
using Exceptionless.DateTimeExtensions;
using Foundatio.Parsers.ElasticQueries.Extensions;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Logging.Abstractions;
using Nest;

namespace Foundatio.Parsers.ElasticQueries;

public class ElasticMappingResolver : IDisposable
{
    private ITypeMapping _serverMapping;
    private readonly ITypeMapping _codeMapping;
    private readonly Inferrer _inferrer;
    private readonly ConcurrentDictionary<string, FieldMapping> _mappingCache = new();
    private readonly object _mappingLock = new();
    private readonly SemaphoreSlim _fetchSemaphore = new(1, 1);
    private long _refreshEpoch;
    private readonly TimeProvider _timeProvider;
    private readonly ILogger _logger;

    public static ElasticMappingResolver NullInstance = new(() => null);

    public ElasticMappingResolver(Func<ITypeMapping> getMapping, Inferrer inferrer = null, TimeProvider timeProvider = null, ILogger logger = null)
    {
        GetServerMappingFunc = getMapping;
        _inferrer = inferrer;
        _timeProvider = timeProvider ?? TimeProvider.System;
        _logger = logger ?? NullLogger.Instance;
    }

    public ElasticMappingResolver(ITypeMapping codeMapping, Inferrer inferrer, Func<ITypeMapping> getMapping, TimeProvider timeProvider = null, ILogger logger = null)
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

        _logger.LogInformation("Mapping refresh triggered.");
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
                if (followAlias && mapping.Property is IFieldAliasProperty fieldAlias)
                {
                    _logger.LogTrace("Cached alias mapping: {Field}={FieldPath}:{FieldType}", field, mapping.FullPath, mapping.Property.Type);
                    return GetMapping(fieldAlias.Path.Name);
                }

                _logger.LogTrace("Cached mapping: {Field}={FieldPath}:{FieldType}", field, mapping.FullPath, mapping.Property?.Type);
                return mapping;
            }

            if (!mapping.Found && mappingCurrent && !GetServerMapping())
            {
                _logger.LogTrace("Cached mapping (not found): {field}=<null>", field);
                return mapping;
            }
        }

        string[] fieldParts = field.Split('.');
        string resolvedFieldName = "";

        // Snapshot server mapping under lock so readers see a consistent pair of
        // _serverMapping + _lastMappingUpdateTicks (both are set together in GetServerMapping).
        ITypeMapping serverMapping;
        DateTime? mappingServerTime;
        lock (_mappingLock)
        {
            serverMapping = _serverMapping;
            long ticks = Interlocked.Read(ref _lastMappingUpdateTicks);
            mappingServerTime = ticks > 0 ? new DateTime(ticks, DateTimeKind.Utc) : null;
        }

        // Lazily fetch server mapping only when none is loaded yet.
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
            if (currentProperties == null || !currentProperties.TryGetValue(fieldPart, out fieldMapping))
            {
                // check to see if there is an name match
                if (currentProperties != null)
                    fieldMapping = currentProperties.Values.FirstOrDefault(m =>
                    {
                        string propertyName = _inferrer.PropertyName(m?.Name);
                        return propertyName != null && propertyName.Equals(fieldPart, StringComparison.OrdinalIgnoreCase);
                    });

                // no mapping found, call GetServerMapping again in case it hasn't been called recently and there are possibly new mappings
                if (fieldMapping == null && GetServerMapping())
                {
                    // Re-snapshot under lock because GetServerMapping updated the shared state.
                    depth = -1;
                    resolvedFieldName = "";
                    lock (_mappingLock)
                    {
                        serverMapping = _serverMapping;
                        long ticks = Interlocked.Read(ref _lastMappingUpdateTicks);
                        mappingServerTime = ticks > 0 ? new DateTime(ticks, DateTimeKind.Utc) : null;
                    }
                    currentProperties = MergeProperties(_codeMapping?.Properties, serverMapping?.Properties);
                    continue;
                }

                if (fieldMapping == null)
                {
                    if (depth == 0)
                        resolvedFieldName += fieldPart;
                    else
                        resolvedFieldName += "." + fieldPart;

                    // mapping is not fully resolved, append the rest of the parts unmodified and break
                    if (fieldParts.Length - 1 > depth)
                    {
                        for (int i = depth + 1; i < fieldParts.Length; i++)
                            resolvedFieldName += "." + fieldParts[i];
                    }

                    break;
                }
            }

            // coded properties sometimes have null Name properties
            if (fieldMapping.Name == null && fieldMapping is IPropertyWithClrOrigin clrOrigin && clrOrigin.ClrOrigin != null)
                fieldMapping.Name = new PropertyName(clrOrigin.ClrOrigin);

            if (depth == 0)
                resolvedFieldName += _inferrer.PropertyName(fieldMapping.Name);
            else
                resolvedFieldName += "." + _inferrer.PropertyName(fieldMapping.Name);

            if (depth == fieldParts.Length - 1)
            {
                var resolvedMapping = new FieldMapping(resolvedFieldName, fieldMapping, mappingServerTime, currentEpoch);
                if (IsSnapshotCurrent(currentEpoch, mappingServerTime))
                    _mappingCache.AddOrUpdate(field, resolvedMapping, (_, existing) =>
                        existing.Epoch > resolvedMapping.Epoch ? existing : resolvedMapping);
                _logger.LogTrace("Resolved mapping: {Field}={FieldPath}:{FieldType}", field, resolvedMapping.FullPath, resolvedMapping.Property?.Type);

                if (followAlias && resolvedMapping.Property is IFieldAliasProperty fieldAlias)
                    return GetMapping(fieldAlias.Path.Name);

                return resolvedMapping;
            }

            if (fieldMapping is IObjectProperty objectProperty)
            {
                currentProperties = objectProperty.Properties;
            }
            else
            {
                if (fieldMapping is ITextProperty textProperty)
                    currentProperties = textProperty.Fields;
                else
                    break;
            }
        }

        _logger.LogTrace("Mapping not found: {field}", field);
        var notFoundMapping = new FieldMapping(resolvedFieldName, null, mappingServerTime, currentEpoch);
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

        var multiFieldProperty = mapping.Property as ICoreProperty;
        if (multiFieldProperty?.Fields == null)
            return mapping.FullPath;

        var nonAnalyzedProperty = multiFieldProperty.Fields.OrderByDescending(kvp => kvp.Key.Name == preferredSubField).FirstOrDefault(kvp =>
        {
            if (kvp.Value is IKeywordProperty)
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
        if (property is ITextProperty textProperty)
            return !textProperty.Index.HasValue || textProperty.Index.Value;

        return false;
    }

    public bool IsNestedPropertyType(string field)
    {
        if (String.IsNullOrEmpty(field))
            return false;

        return GetMappingProperty(field, true) is INestedProperty;
    }

    public bool IsGeoPropertyType(string field)
    {
        if (String.IsNullOrEmpty(field))
            return false;

        return GetMappingProperty(field, true) is IGeoPointProperty;
    }

    public bool IsNumericPropertyType(string field)
    {
        if (String.IsNullOrEmpty(field))
            return false;

        return GetMappingProperty(field, true) is INumberProperty;
    }

    public bool IsBooleanPropertyType(string field)
    {
        if (String.IsNullOrEmpty(field))
            return false;

        return GetMappingProperty(field, true) is IBooleanProperty;
    }

    public bool IsDatePropertyType(string field)
    {
        if (String.IsNullOrEmpty(field))
            return false;

        return GetMappingProperty(field, true) is IDateProperty;
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
            "geo_point" => FieldType.GeoPoint,
            "geo_shape" => FieldType.GeoShape,
            "ip" => FieldType.Ip,
            "binary" => FieldType.Binary,
            "keyword" => FieldType.Keyword,
            "string" or "text" => FieldType.Text,
            "date" => FieldType.Date,
            "boolean" => FieldType.Boolean,
            "completion" => FieldType.Completion,
            "nested" => FieldType.Nested,
            "object" => FieldType.Object,
            "murmur3" => FieldType.Murmur3Hash,
            "token_count" => FieldType.TokenCount,
            "percolator" => FieldType.Percolator,
            "integer" => FieldType.Integer,
            "long" => FieldType.Long,
            "short" => FieldType.Short,
            "byte" => FieldType.Byte,
            "float" => FieldType.Float,
            "half_float" => FieldType.HalfFloat,
            "scaled_float" => FieldType.ScaledFloat,
            "double" => FieldType.Double,
            "integer_range" => FieldType.IntegerRange,
            "float_range" => FieldType.FloatRange,
            "long_range" => FieldType.LongRange,
            "double_range" => FieldType.DoubleRange,
            "date_range" => FieldType.DateRange,
            "ip_range" => FieldType.IpRange,
            _ => FieldType.None,
        };
    }

    private IProperties MergeProperties(IProperties codeProperties, IProperties serverProperties)
    {
        if (codeProperties == null && serverProperties == null)
            return null;

        IProperties mergedCodeProperties = null;
        // resolve code mapping property expressions using inferrer
        if (codeProperties != null)
        {
            mergedCodeProperties = new Properties();

            foreach (var kvp in codeProperties)
            {
                var propertyName = kvp.Key;
                if (_inferrer != null && (String.IsNullOrEmpty(kvp.Key.Name) || kvp.Value is IFieldAliasProperty))
                    propertyName = _inferrer.PropertyName(kvp.Key) ?? kvp.Key;

                mergedCodeProperties[propertyName] = kvp.Value;
            }

            if (_inferrer != null)
            {
                // resolve field alias
                foreach (var kvp in codeProperties)
                {
                    if (kvp.Value is not IFieldAliasProperty aliasProperty)
                        continue;

                    mergedCodeProperties[kvp.Key] = new FieldAliasProperty
                    {
                        LocalMetadata = aliasProperty.LocalMetadata,
                        Path = _inferrer?.Field(aliasProperty.Path) ?? aliasProperty.Path,
                        Name = aliasProperty.Name
                    };
                }
            }
        }

        // no need to merge
        if (mergedCodeProperties == null || serverProperties == null)
            return mergedCodeProperties ?? serverProperties;

        IProperties properties = new Properties();
        foreach (var serverProperty in serverProperties)
        {
            var merged = serverProperty.Value;
            if (mergedCodeProperties.TryGetValue(serverProperty.Key, out var codeProperty))
                merged.LocalMetadata = codeProperty.LocalMetadata;

            switch (merged)
            {
                case IObjectProperty objectProperty:
                    var codeObjectProperty = codeProperty as IObjectProperty;
                    objectProperty.Properties = MergeProperties(codeObjectProperty?.Properties, objectProperty.Properties);
                    break;
                case ITextProperty textProperty:
                    var codeTextProperty = codeProperty as ITextProperty;
                    textProperty.Fields = MergeProperties(codeTextProperty?.Fields, textProperty.Fields);
                    break;
            }

            properties.Add(serverProperty.Key, merged);
        }

        foreach (var codeProperty in mergedCodeProperties)
        {
            if (properties.TryGetValue(codeProperty.Key, out _))
                continue;

            properties.Add(codeProperty.Key, codeProperty.Value);
        }

        return properties;
    }

    private Func<ITypeMapping> GetServerMappingFunc { get; set; }
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

            ITypeMapping newMapping;
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

    public static ElasticMappingResolver Create<T>(Func<TypeMappingDescriptor<T>, ITypeMapping> mappingBuilder, IElasticClient client, ILogger logger = null) where T : class
    {
        logger ??= NullLogger.Instance;

        return Create(mappingBuilder, client.Infer, () =>
        {
            var response = client.Indices.GetMapping(new GetMappingRequest(Indices.Index<T>()));
            logger.LogTrace("GetMapping: {Request}", response.GetRequest(false, true));

            // use first returned mapping because index could have been an index alias
            var mapping = response.Indices.Values.FirstOrDefault()?.Mappings;
            return mapping;
        }, logger);
    }

    public static ElasticMappingResolver Create<T>(Func<TypeMappingDescriptor<T>, ITypeMapping> mappingBuilder, IElasticClient client, string index, ILogger logger = null) where T : class
    {
        logger ??= NullLogger.Instance;

        return Create(mappingBuilder, client.Infer, () =>
        {
            var response = client.Indices.GetMapping(new GetMappingRequest(index));
            logger.LogTrace("GetMapping: {Request}", response.GetRequest(false, true));

            // use first returned mapping because index could have been an index alias
            var mapping = response.Indices.Values.FirstOrDefault()?.Mappings;
            return mapping;
        }, logger);
    }

    public static ElasticMappingResolver Create<T>(Func<TypeMappingDescriptor<T>, ITypeMapping> mappingBuilder, Inferrer inferrer, Func<ITypeMapping> getMapping, ILogger logger = null) where T : class
    {
        var codeMapping = new TypeMappingDescriptor<T>();
        codeMapping = mappingBuilder(codeMapping) as TypeMappingDescriptor<T>;
        return new ElasticMappingResolver(codeMapping, inferrer, getMapping, logger: logger);
    }

    public static ElasticMappingResolver Create<T>(IElasticClient client, ILogger logger = null)
    {
        logger ??= NullLogger.Instance;

        return Create(() =>
        {
            var response = client.Indices.GetMapping(new GetMappingRequest(Indices.Index<T>()));
            logger.LogTrace("GetMapping: {Request}", response.GetRequest(false, true));

            // use first returned mapping because index could have been an index alias
            var mapping = response.Indices.Values.FirstOrDefault()?.Mappings;
            return mapping;
        }, client.Infer, logger);
    }

    public static ElasticMappingResolver Create(IElasticClient client, string index, ILogger logger = null)
    {
        logger ??= NullLogger.Instance;

        return Create(() =>
        {
            var response = client.Indices.GetMapping(new GetMappingRequest(index));
            logger.LogTrace("GetMapping: {Request}", response.GetRequest(false, true));

            // use first returned mapping because index could have been an index alias
            var mapping = response.Indices.Values.FirstOrDefault()?.Mappings;
            return mapping;
        }, client.Infer, logger);
    }

    public static ElasticMappingResolver Create(Func<ITypeMapping> getMapping, Inferrer inferrer, ILogger logger = null)
    {
        return new ElasticMappingResolver(getMapping, inferrer, logger: logger);
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
