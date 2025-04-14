using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Linq;
using Elastic.Clients.Elasticsearch;
using Elastic.Clients.Elasticsearch.IndexManagement;
using Elastic.Clients.Elasticsearch.Mapping;
using Exceptionless.DateTimeExtensions;
using Foundatio.Parsers.ElasticQueries.Extensions;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Logging.Abstractions;

namespace Foundatio.Parsers.ElasticQueries;

public class ElasticMappingResolver
{
    private TypeMapping _serverMapping;
    private readonly TypeMapping _codeMapping;
    private readonly Inferrer _inferrer;
    private readonly ConcurrentDictionary<string, FieldMapping> _mappingCache = new();
    private readonly ILogger _logger;

    public static ElasticMappingResolver NullInstance = new(() => null);

    public ElasticMappingResolver(Func<TypeMapping> getMapping, Inferrer inferrer = null, ILogger logger = null)
    {
        GetServerMappingFunc = getMapping;
        _inferrer = inferrer;
        _logger = logger ?? NullLogger.Instance;
    }

    public ElasticMappingResolver(TypeMapping codeMapping, Inferrer inferrer, Func<TypeMapping> getMapping, ILogger logger = null)
        : this(getMapping, inferrer, logger)
    {
        _codeMapping = codeMapping;
    }

    /// <summary>
    /// Allows you to refresh server side mapping. This should be used only in unit tests.
    /// </summary>
    public void RefreshMapping()
    {
        _logger.LogInformation("Mapping refresh triggered");
        _serverMapping = null;
        _lastMappingUpdate = null;
    }

    public FieldMapping GetMapping(string field, bool followAlias = false)
    {
        if (String.IsNullOrWhiteSpace(field))
            return null;

        if (GetServerMappingFunc == null && _codeMapping == null)
            throw new InvalidOperationException("No mappings are available.");

        if (_mappingCache.TryGetValue(field, out var mapping))
        {
            if (followAlias && mapping.Found && mapping.Property is FieldAliasProperty fieldAlias)
            {
                _logger.LogTrace("Cached alias mapping: {Field}={FieldPath}:{FieldType}", field, mapping.FullPath, mapping.Property?.Type);
                return GetMapping(fieldAlias.Path.Name);
            }

            if (mapping.Found)
            {
                _logger.LogTrace("Cached mapping: {Field}={FieldPath}:{FieldType}", field, mapping.FullPath, mapping.Property?.Type);
                return mapping;
            }

            if (mapping.ServerMapTime >= _lastMappingUpdate && !GetServerMapping())
            {
                _logger.LogTrace("Cached mapping (not found): {Field}=<null>", field);
                return mapping;
            }

            _logger.LogTrace("Cached mapping (not found), got new server mapping");
        }

        string[] fieldParts = field.Split('.');
        string resolvedFieldName = "";
        var mappingServerTime = _lastMappingUpdate;
        var currentProperties = MergeProperties(_codeMapping?.Properties, _serverMapping?.Properties);

        for (int depth = 0; depth < fieldParts.Length; depth++)
        {
            string fieldPart = fieldParts[depth];
            IProperty fieldMapping = null;
            if (currentProperties == null || !currentProperties.TryGetProperty(fieldPart, out fieldMapping))
            {
                // check to see if there is a name match
                if (currentProperties != null)
                    fieldMapping = ((IDictionary<PropertyName, IProperty>)currentProperties).Values.FirstOrDefault(m =>
                    {
                        string propertyName = _inferrer.PropertyName(m?.TryGetName());
                        return propertyName != null && propertyName.Equals(fieldPart, StringComparison.OrdinalIgnoreCase);
                    });

                // no mapping found, call GetServerMapping again in case it hasn't been called recently and there are possibly new mappings
                if (fieldMapping == null && GetServerMapping())
                {
                    // we got updated mapping, start over from the top
                    depth = -1;
                    resolvedFieldName = "";
                    currentProperties = MergeProperties(_codeMapping?.Properties, _serverMapping?.Properties);
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
            string name = fieldMapping.TryGetName();
            // TODO: ?
            // if (name == null && fieldMapping is IPropertyWithClrOrigin clrOrigin && clrOrigin.ClrOrigin != null)
            //     name = new PropertyName(clrOrigin.ClrOrigin);

            if (depth == 0)
                resolvedFieldName += _inferrer.PropertyName(name);
            else
                resolvedFieldName += "." + _inferrer.PropertyName(name);

            if (depth == fieldParts.Length - 1)
            {
                var resolvedMapping = new FieldMapping(resolvedFieldName, fieldMapping, mappingServerTime);
                _mappingCache.AddOrUpdate(field, resolvedMapping, (_, _) => resolvedMapping);
                _logger.LogTrace("Resolved mapping: {Field}={FieldPath}:{FieldType}", field, resolvedMapping.FullPath, resolvedMapping.Property?.Type);

                if (followAlias && resolvedMapping.Property is FieldAliasProperty fieldAlias)
                    return GetMapping(fieldAlias.Path.Name);

                return resolvedMapping;
            }

            if (fieldMapping is ObjectProperty objectProperty)
            {
                currentProperties = objectProperty.Properties;
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
        var notFoundMapping = new FieldMapping(resolvedFieldName, null, mappingServerTime);
        _mappingCache.AddOrUpdate(field, notFoundMapping, (_, _) => notFoundMapping);

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

        return GetMappingProperty(field, true) is DateProperty;
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
            "version"=> FieldType.Version,
			"token_count"=> FieldType.TokenCount,
			"text"=> FieldType.Text,
			"sparse_vector"=> FieldType.SparseVector,
			"short"=> FieldType.Short,
			"shape"=> FieldType.Shape,
			"semantic_text"=> FieldType.SemanticText,
			"search_as_you_type"=> FieldType.SearchAsYouType,
			"scaled_float"=> FieldType.ScaledFloat,
			"rank_features"=> FieldType.RankFeatures,
			"rank_feature"=> FieldType.RankFeature,
			"percolator"=> FieldType.Percolator,
			"object"=> FieldType.Object,
			"none"=> FieldType.None,
			"nested"=> FieldType.Nested,
			"murmur3"=> FieldType.Murmur3,
			"match_only_text"=> FieldType.MatchOnlyText,
			"long_range"=> FieldType.LongRange,
			"long"=> FieldType.Long,
			"keyword"=> FieldType.Keyword,
			"join"=> FieldType.Join,
			"ip_range"=> FieldType.IpRange,
			"ip"=> FieldType.Ip,
			"integer_range"=> FieldType.IntegerRange,
			"integer"=> FieldType.Integer,
			"icu_collation_keyword"=> FieldType.IcuCollationKeyword,
			"histogram"=> FieldType.Histogram,
			"half_float"=> FieldType.HalfFloat,
			"geo_shape"=> FieldType.GeoShape,
			"geo_point"=> FieldType.GeoPoint,
			"float_range"=> FieldType.FloatRange,
			"float"=> FieldType.Float,
			"flattened"=> FieldType.Flattened,
			"double_range"=> FieldType.DoubleRange,
			"double"=> FieldType.Double,
			"dense_vector"=> FieldType.DenseVector,
			"date_range"=> FieldType.DateRange,
			"date_nanos"=> FieldType.DateNanos,
			"date"=> FieldType.Date,
			"constant_keyword"=> FieldType.ConstantKeyword,
			"completion"=> FieldType.Completion,
			"byte"=> FieldType.Byte,
			"boolean"=> FieldType.Boolean,
			"binary"=> FieldType.Binary,
			"alias"=> FieldType.Alias,
			"aggregate_metric_double"=> FieldType.AggregateMetricDouble,
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
                if (_inferrer != null && (String.IsNullOrEmpty(kvp.Key.Name) || kvp.Value is FieldAliasProperty))
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

                    mergedCodeProperties[kvp.Key] = new FieldAliasProperty
                    {
                        //LocalMetadata = aliasProperty.LocalMetadata,
                        Path = _inferrer?.Field(aliasProperty.Path) ?? aliasProperty.Path,
                        // Name = aliasProperty.Name
                    };
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
            // if (mergedCodeProperties.TryGetProperty(serverProperty.Key, out var codeProperty))
            //     merged.LocalMetadata = codeProperty.LocalMetadata;

            if (mergedCodeProperties.TryGetProperty(serverProperty.Key, out var codeProperty))
            {
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
    private DateTime? _lastMappingUpdate = null;
    private bool GetServerMapping()
    {
        if (GetServerMappingFunc == null)
            return false;

        if (_lastMappingUpdate.HasValue && _lastMappingUpdate.Value > DateTime.UtcNow.SubtractMinutes(1))
            return false;

        try
        {
            _serverMapping = GetServerMappingFunc();
            _lastMappingUpdate = DateTime.UtcNow;
            _logger.LogInformation("Got server mapping");

            return true;
        }
        catch (Exception ex)
        {
            _logger.LogError(ex, "Error getting server mapping: {Message}", ex.Message);
            return false;
        }
    }

    public static ElasticMappingResolver Create<T>(Func<TypeMappingDescriptor<T>, TypeMapping> mappingBuilder, ElasticsearchClient client, ILogger logger = null) where T : class
    {
        logger ??= NullLogger.Instance;

        return Create(mappingBuilder, client.Infer, () =>
        {
            client.Indices.Refresh(Indices.Index<T>());
            var response = client.Indices.GetMapping(new GetMappingRequest(Indices.Index<T>()));
            logger.LogTrace("GetMapping: {Request}", response.GetRequest(false, true));

            // use first returned mapping because index could have been an index alias
            var mapping = response.Mappings.Values.FirstOrDefault()?.Mappings;
            return mapping;
        }, logger);
    }

    public static ElasticMappingResolver Create<T>(Func<TypeMappingDescriptor<T>, TypeMapping> mappingBuilder, ElasticsearchClient client, string index, ILogger logger = null) where T : class
    {
        logger ??= NullLogger.Instance;

        return Create(mappingBuilder, client.Infer, () =>
        {
            client.Indices.Refresh(index);
            var response = client.Indices.GetMapping(new GetMappingRequest(index));
            logger.LogTrace("GetMapping: {Request}", response.GetRequest(false, true));

            // use first returned mapping because index could have been an index alias
            var mapping = response.Mappings.Values.FirstOrDefault()?.Mappings;
            return mapping;
        }, logger);
    }

    public static ElasticMappingResolver Create<T>(Func<TypeMappingDescriptor<T>, TypeMapping> mappingBuilder, Inferrer inferrer, Func<TypeMapping> getMapping, ILogger logger = null) where T : class
    {
        var codeMapping = mappingBuilder(new TypeMappingDescriptor<T>());
        return new ElasticMappingResolver(codeMapping, inferrer, getMapping, logger: logger);
    }

public static ElasticMappingResolver Create<T>(ElasticsearchClient client, ILogger logger = null)
{
    logger ??= NullLogger.Instance;

    return Create(() =>
    {
        client.Indices.Refresh(Indices.Index<T>());
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
            client.Indices.Refresh(index);
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
}

public class FieldMapping
{
    public FieldMapping(string path, IProperty property, DateTime? serverMapTime)
    {
        FullPath = path;
        Property = property;
        ServerMapTime = serverMapTime;
    }

    public bool Found => Property != null;
    public string FullPath { get; private set; }
    public IProperty Property { get; private set; }
    public DateTime Date { get; private set; } = DateTime.Now;
    internal DateTime? ServerMapTime { get; private set; }
}
