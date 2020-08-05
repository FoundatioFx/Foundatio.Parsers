using System;
using System.Collections.Concurrent;
using System.Linq;
using Exceptionless.DateTimeExtensions;
using Foundatio.Parsers.ElasticQueries.Extensions;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Logging.Abstractions;
using Nest;

namespace Foundatio.Parsers.ElasticQueries {
    public class ElasticMappingResolver {
        private ITypeMapping _serverMapping;
        private readonly ITypeMapping _codeMapping;
        private readonly Inferrer _inferrer;
        private readonly ConcurrentDictionary<string, IProperty> _propertyCache = new ConcurrentDictionary<string, IProperty>();
        private readonly ILogger _logger;

        public static ElasticMappingResolver NullInstance = new ElasticMappingResolver(() => null);

        public ElasticMappingResolver(Func<ITypeMapping> getMapping, Inferrer inferrer = null, ILogger logger = null) {
            GetServerMappingFunc = getMapping;
            _inferrer = inferrer;
            _logger = logger ?? NullLogger.Instance;
        }

        public ElasticMappingResolver(ITypeMapping codeMapping, Inferrer inferrer, Func<ITypeMapping> getMapping, ILogger logger = null)
            : this(getMapping, inferrer, logger) {
            _codeMapping = codeMapping;
        }

        /// <summary>
        /// Allows you to refresh server side mapping. This should be used only in unit tests.
        /// </summary>
        public void RefreshMapping() {
            _serverMapping = null;
            _lastMappingUpdate = null;
        }

        public IProperty GetMappingProperty(Field field) {
            if (_inferrer == null)
                throw new InvalidOperationException("Unable to resolve Field without inferrer");

            return GetMappingProperty(_inferrer.Field(field));
        }

        public IProperty GetMappingProperty(string field) {
            if (String.IsNullOrEmpty(field))
                return null;

            if (_propertyCache.TryGetValue(field.ToLowerInvariant(), out var propertyType)) {
                _logger.LogTrace("Cached property mapping: {field}={type}", field, propertyType);
                return propertyType;
            }

            if (GetServerMappingFunc == null && _codeMapping == null) {
                _logger.LogTrace("No property mappings are available");
                return null;
            }

            var fieldParts = field.Split('.');
            var currentProperties = MergeProperties(_codeMapping?.Properties, _serverMapping?.Properties);

            for (int depth = 0; depth < fieldParts.Length; depth++) {
                string fieldPart = fieldParts[depth];
                IProperty fieldMapping = null;
                if (currentProperties == null || !currentProperties.TryGetValue(fieldPart, out fieldMapping)) {
                    // check to see if there is an name match
                    if (currentProperties != null)
                        fieldMapping = currentProperties.Values.FirstOrDefault(m => m.Name == fieldPart || (m.Name?.Name != null && m.Name.Name.Equals(fieldPart, StringComparison.OrdinalIgnoreCase)));

                    // no mapping found, call GetServerMapping again in case it hasn't been called recently and there are possibly new mappings
                    if (fieldMapping == null && GetServerMapping()) {
                        // we got updated mapping, start over from the top
                        depth = -1;
                        currentProperties = MergeProperties(_codeMapping?.Properties, _serverMapping?.Properties);
                        continue;
                    }

                    if (fieldMapping == null)
                        return null;
                }

                if (depth == fieldParts.Length - 1) {
                    _propertyCache.TryAdd(field, fieldMapping);
                    _logger.LogTrace("Property mapping: {field}={fieldMapping}", field, fieldMapping);
                    return fieldMapping;
                }

                if (fieldMapping is IObjectProperty objectProperty) {
                    currentProperties = objectProperty.Properties;
                } else {
                    if (fieldMapping is ITextProperty textProperty)
                        currentProperties = textProperty.Fields;
                    else
                        return null;
                }
            }

            _logger.LogTrace("Property mapping: {field}=<null>", field);
            return null;
        }

        public string GetResolvedField(Field field) {
            if (_inferrer == null)
                throw new InvalidOperationException("Unable to resolve Field without inferrer");

            return GetResolvedField(_inferrer.Field(field));
        }

        public string GetResolvedField(string field) {
            var result = GetResolvedMappingProperty(field);
            return result.ResolvedField;
        }

        public (string ResolvedField, IProperty Mapping) GetResolvedMappingProperty(string field) {
            if (String.IsNullOrEmpty(field))
                return (field, null);

            string resolvedField = field;
            var property = GetMappingProperty(field);

            if (property is IFieldAliasProperty fieldAlias) {
                resolvedField = fieldAlias.Path.Name;
                property = GetMappingProperty(resolvedField);
            }

            return (resolvedField, property);
        }

        public string GetSortFieldName(string field) {
            return GetNonAnalyzedFieldName(field, ElasticMapping.SortFieldName);
        }

        public string GetSortFieldName(Field field) {
            return GetNonAnalyzedFieldName(GetResolvedField(field), ElasticMapping.SortFieldName);
        }

        public string GetAggregationsFieldName(string field) {
            return GetNonAnalyzedFieldName(field, ElasticMapping.KeywordFieldName);
        }

        public string GetAggregationsFieldName(Field field) {
            return GetNonAnalyzedFieldName(GetResolvedField(field), ElasticMapping.KeywordFieldName);
        }

        public string GetNonAnalyzedFieldName(string field, string preferredSubField = null) {
            if (String.IsNullOrEmpty(field))
                return field;

            var property = GetResolvedMappingProperty(field);

            if (property.Mapping == null || !IsPropertyAnalyzed(property.Mapping))
                return field;

            var multiFieldProperty = property.Mapping as ICoreProperty;
            if (multiFieldProperty?.Fields == null)
                return property.ResolvedField;

            var nonAnalyzedProperty = multiFieldProperty.Fields.OrderByDescending(kvp => kvp.Key.Name == preferredSubField).FirstOrDefault(kvp => {
                if (kvp.Value is IKeywordProperty)
                    return true;

                if (!IsPropertyAnalyzed(kvp.Value))
                    return true;

                return false;
            });

            if (nonAnalyzedProperty.Value != null)
                return property.ResolvedField + "." + nonAnalyzedProperty.Key.Name;

            return property.ResolvedField;
        }

        public bool IsPropertyAnalyzed(string field) {
            // assume default is analyzed
            if (String.IsNullOrEmpty(field))
                return true;

            var property = GetResolvedMappingProperty(field);
            if (property.Mapping == null)
                return false;

            return IsPropertyAnalyzed(property.Mapping);
        }

        public bool IsPropertyAnalyzed(IProperty property) {
            if (property is ITextProperty textProperty)
                return !textProperty.Index.HasValue || textProperty.Index.Value;

            return false;
        }

        public bool IsNestedPropertyType(string field) {
            if (String.IsNullOrEmpty(field))
                return false;

            return GetResolvedMappingProperty(field).Mapping is INestedProperty;
        }

        public bool IsGeoPropertyType(string field) {
            if (String.IsNullOrEmpty(field))
                return false;

            return GetResolvedMappingProperty(field).Mapping is IGeoPointProperty;
        }

        public bool IsNumericPropertyType(string field) {
            if (String.IsNullOrEmpty(field))
                return false;

            return GetResolvedMappingProperty(field).Mapping is INumberProperty;
        }

        public bool IsBooleanPropertyType(string field) {
            if (String.IsNullOrEmpty(field))
                return false;

            return GetResolvedMappingProperty(field).Mapping is IBooleanProperty;
        }

        public bool IsDatePropertyType(string field) {
            if (String.IsNullOrEmpty(field))
                return false;

            return GetResolvedMappingProperty(field).Mapping is IDateProperty;
        }

        public FieldType GetFieldType(string field) {
            if (String.IsNullOrWhiteSpace(field))
                return FieldType.None;

            var property = GetResolvedMappingProperty(field);

            if (property.Mapping?.Type == null)
                return FieldType.None;

            switch (property.Mapping.Type) {
                case "geo_point":
                    return FieldType.GeoPoint;
                case "geo_shape":
                    return FieldType.GeoShape;
                case "ip":
                    return FieldType.Ip;
                case "binary":
                    return FieldType.Binary;
                case "keyword":
                    return FieldType.Keyword;
                case "string":
                case "text":
                    return FieldType.Text;
                case "date":
                    return FieldType.Date;
                case "boolean":
                    return FieldType.Boolean;
                case "completion":
                    return FieldType.Completion;
                case "nested":
                    return FieldType.Nested;
                case "object":
                    return FieldType.Object;
                case "murmur3":
                    return FieldType.Murmur3Hash;
                case "token_count":
                    return FieldType.TokenCount;
                case "percolator":
                    return FieldType.Percolator;
                case "integer":
                    return FieldType.Integer;
                case "long":
                    return FieldType.Long;
                case "short":
                    return FieldType.Short;
                case "byte":
                    return FieldType.Byte;
                case "float":
                    return FieldType.Float;
                case "half_float":
                    return FieldType.HalfFloat;
                case "scaled_float":
                    return FieldType.ScaledFloat;
                case "double":
                    return FieldType.Double;
                case "integer_range":
                    return FieldType.IntegerRange;
                case "float_range":
                    return FieldType.FloatRange;
                case "long_range":
                    return FieldType.LongRange;
                case "double_range":
                    return FieldType.DoubleRange;
                case "date_range":
                    return FieldType.DateRange;
                case "ip_range":
                    return FieldType.IpRange;
                default:
                    return FieldType.None;
            }
        }

        private IProperties MergeProperties(IProperties codeProperties, IProperties serverProperties) {
            if (codeProperties == null && serverProperties == null)
                return null;

            IProperties mergedCodeProperties = null;
            // resolve code mapping property expressions using inferrer
            if (codeProperties != null) {
                mergedCodeProperties = new Properties();

                foreach (var kvp in codeProperties) {
                    var propertyName = kvp.Key;
                    if (_inferrer != null && (String.IsNullOrEmpty(kvp.Key.Name) || kvp.Value is IFieldAliasProperty))
                        propertyName = _inferrer.PropertyName(kvp.Key) ?? kvp.Key;

                    mergedCodeProperties[propertyName] = kvp.Value;
                }

                if (_inferrer != null) {
                    // resolve field alias
                    foreach (var kvp in codeProperties) {
                        if (!(kvp.Value is IFieldAliasProperty aliasProperty))
                            continue;

                        mergedCodeProperties[kvp.Key] = new FieldAliasProperty {
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
            foreach (var serverProperty in serverProperties) {
                var merged = serverProperty.Value;
                if (mergedCodeProperties.TryGetValue(serverProperty.Key, out var codeProperty))
                    merged.LocalMetadata = codeProperty.LocalMetadata;

                switch (merged) {
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

            foreach (var codeProperty in mergedCodeProperties) {
                if (properties.TryGetValue(codeProperty.Key, out _))
                    continue;

                properties.Add(codeProperty.Key, codeProperty.Value);
            }

            return properties;
        }

        private Func<ITypeMapping> GetServerMappingFunc { get; set; }
        private DateTime? _lastMappingUpdate = null;
        private bool GetServerMapping() {
            if (GetServerMappingFunc == null)
                return false;

            if (_lastMappingUpdate.HasValue && _lastMappingUpdate.Value > DateTime.UtcNow.SubtractMinutes(1))
                return false;

            try {
                _serverMapping = GetServerMappingFunc();
                _logger.LogTrace("Got server mapping {mapping}", _serverMapping);
                _lastMappingUpdate = DateTime.UtcNow;

                return true;
            } catch (Exception ex) {
                _logger.LogError(ex, "Error getting server mapping: " + ex.Message);
                return false;
            }
        }

        public static ElasticMappingResolver Create<T>(Func<TypeMappingDescriptor<T>, TypeMappingDescriptor<T>> mappingBuilder, IElasticClient client, string index, ILogger logger = null) where T : class {
            logger = logger ?? NullLogger.Instance;

            return Create(mappingBuilder, client.Infer, () => {
                var response = client.Indices.GetMapping(new GetMappingRequest(index));
                logger.LogTrace("GetMapping: {Request}", response.GetRequest(false, true));

                // use first returned mapping because index could have been an index alias
                var mapping = response.Indices.Values.FirstOrDefault()?.Mappings;
                return mapping;
            });
        }

        public static ElasticMappingResolver Create<T>(Func<TypeMappingDescriptor<T>, TypeMappingDescriptor<T>> mappingBuilder, Inferrer inferrer, Func<ITypeMapping> getMapping, ILogger logger = null) where T : class {
            var codeMapping = new TypeMappingDescriptor<T>();
            codeMapping = mappingBuilder(codeMapping);
            return new ElasticMappingResolver(codeMapping, inferrer, getMapping, logger: logger);
        }

        public static ElasticMappingResolver Create<T>(IElasticClient client, ILogger logger = null) {
            logger = logger ?? NullLogger.Instance;

            return Create(() => {
                var response = client.Indices.GetMapping(new GetMappingRequest(Indices.Index<T>()));
                logger.LogTrace("GetMapping: {Request}", response.GetRequest(false, true));

                // use first returned mapping because index could have been an index alias
                var mapping = response.Indices.Values.FirstOrDefault()?.Mappings;
                return mapping;
            }, client.Infer);
        }

        public static ElasticMappingResolver Create(IElasticClient client, string index, ILogger logger = null) {
            logger = logger ?? NullLogger.Instance;

            return Create(() => {
                var response = client.Indices.GetMapping(new GetMappingRequest(index));
                logger.LogTrace("GetMapping: {Request}", response.GetRequest(false, true));

                // use first returned mapping because index could have been an index alias
                var mapping = response.Indices.Values.FirstOrDefault()?.Mappings;
                return mapping;
            }, client.Infer);
        }

        public static ElasticMappingResolver Create(Func<ITypeMapping> getMapping, Inferrer inferrer, ILogger logger = null) {
            return new ElasticMappingResolver(getMapping, inferrer, logger: logger);
        }
    }
}
