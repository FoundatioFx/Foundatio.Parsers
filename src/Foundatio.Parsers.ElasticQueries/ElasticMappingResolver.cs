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
        private readonly ConcurrentDictionary<string, FieldMapping> _mappingCache = new ConcurrentDictionary<string, FieldMapping>();
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
            _logger.LogInformation("Mapping refresh triggered.");
            _serverMapping = null;
            _lastMappingUpdate = null;
        }

        public FieldMapping GetMapping(string field, bool followAlias = false) {
            if (String.IsNullOrEmpty(field))
                return null;

            if (GetServerMappingFunc == null && _codeMapping == null)
                throw new InvalidOperationException("No mappings are available.");

            if (_mappingCache.TryGetValue(field.ToLowerInvariant(), out var mapping)) {

                if (followAlias && mapping.Found && mapping.Property is IFieldAliasProperty fieldAlias) {
                    _logger.LogTrace("Cached alias mapping: {Field}={FieldPath}:{FieldType}", field, mapping.FullPath, mapping.Property?.Type);
                    return GetMapping(fieldAlias.Path.Name);
                }

                if (mapping.Found) {
                    _logger.LogTrace("Cached mapping: {Field}={FieldPath}:{FieldType}", field, mapping.FullPath, mapping.Property?.Type);
                    return mapping;
                }

                if (mapping.ServerMapTime >= _lastMappingUpdate && !GetServerMapping()) {
                    _logger.LogTrace("Cached mapping (not found): {field}=<null>", field);
                    return mapping;
                }

                _logger.LogTrace("Cached mapping (not found), got new server mapping.");
            }

            var fieldParts = field.Split('.');
            string resolvedFieldName = "";
            var mappingServerTime = _lastMappingUpdate;
            var currentProperties = MergeProperties(_codeMapping?.Properties, _serverMapping?.Properties);

            for (int depth = 0; depth < fieldParts.Length; depth++) {
                string fieldPart = fieldParts[depth];
                IProperty fieldMapping = null;
                if (currentProperties == null || !currentProperties.TryGetValue(fieldPart, out fieldMapping)) {
                    // check to see if there is an name match
                    if (currentProperties != null)
                        fieldMapping = currentProperties.Values.FirstOrDefault(m => {
                            var propertyName = _inferrer.PropertyName(m?.Name);
                            return propertyName == null ? false : propertyName.Equals(fieldPart, StringComparison.OrdinalIgnoreCase);
                        });

                    // no mapping found, call GetServerMapping again in case it hasn't been called recently and there are possibly new mappings
                    if (fieldMapping == null && GetServerMapping()) {
                        // we got updated mapping, start over from the top
                        depth = -1;
                        resolvedFieldName = "";
                        currentProperties = MergeProperties(_codeMapping?.Properties, _serverMapping?.Properties);
                        continue;
                    }

                    if (fieldMapping == null) {
                        if (depth == 0)
                            resolvedFieldName += fieldPart;
                        else
                            resolvedFieldName += "." + fieldPart;

                        break;
                    }
                }

                if (depth == 0)
                    resolvedFieldName += _inferrer.PropertyName(fieldMapping.Name);
                else
                    resolvedFieldName += "." + _inferrer.PropertyName(fieldMapping.Name);

                if (depth == fieldParts.Length - 1) {
                    var resolvedMapping = new FieldMapping(resolvedFieldName, fieldMapping, mappingServerTime);
                    _mappingCache.AddOrUpdate(field.ToLowerInvariant(), resolvedMapping, (f, m) => resolvedMapping);
                    _logger.LogTrace("Resolved mapping: {Field}={FieldPath}:{FieldType}", field, resolvedMapping.FullPath, resolvedMapping.Property?.Type);

                    if (followAlias && resolvedMapping.Property is IFieldAliasProperty fieldAlias)
                        return GetMapping(fieldAlias.Path.Name);

                    return resolvedMapping;
                }

                if (fieldMapping is IObjectProperty objectProperty) {
                    currentProperties = objectProperty.Properties;
                } else {
                    if (fieldMapping is ITextProperty textProperty)
                        currentProperties = textProperty.Fields;
                    else
                        break;
                }
            }

            _logger.LogTrace("Mapping not found: {field}", field);
            var notFoundMapping = new FieldMapping(resolvedFieldName, null, mappingServerTime);
            _mappingCache.AddOrUpdate(field.ToLowerInvariant(), notFoundMapping, (f, m) => notFoundMapping);

            return notFoundMapping;
        }

        public FieldMapping GetMapping(Field field, bool followAlias = false) {
            if (_inferrer == null)
                throw new InvalidOperationException("Unable to resolve Field without inferrer");

            return GetMapping(_inferrer.Field(field), followAlias);
        }

        public IProperty GetMappingProperty(string field, bool followAlias = false) {
            return GetMapping(field, followAlias).Property;
        }

        public IProperty GetMappingProperty(Field field, bool followAlias = false) {
            return GetMapping(field, followAlias).Property;
        }

        public string GetResolvedField(string field) {
            var result = GetMapping(field, true);
            return result.FullPath;
        }

        public string GetResolvedField(Field field) {
            if (_inferrer == null)
                throw new InvalidOperationException("Unable to resolve Field without inferrer");

            return GetResolvedField(_inferrer.Field(field));
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

            var mapping = GetMapping(field, true);

            if (mapping.Property == null || !IsPropertyAnalyzed(mapping.Property))
                return field;

            var multiFieldProperty = mapping.Property as ICoreProperty;
            if (multiFieldProperty?.Fields == null)
                return mapping.FullPath;

            var nonAnalyzedProperty = multiFieldProperty.Fields.OrderByDescending(kvp => kvp.Key.Name == preferredSubField).FirstOrDefault(kvp => {
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

        public bool IsPropertyAnalyzed(string field) {
            // assume default is analyzed
            if (String.IsNullOrEmpty(field))
                return true;

            var property = GetMapping(field, true);
            if (!property.Found)
                return false;

            return IsPropertyAnalyzed(property.Property);
        }

        public bool IsPropertyAnalyzed(IProperty property) {
            if (property is ITextProperty textProperty)
                return !textProperty.Index.HasValue || textProperty.Index.Value;

            return false;
        }

        public bool IsNestedPropertyType(string field) {
            if (String.IsNullOrEmpty(field))
                return false;

            return GetMappingProperty(field, true) is INestedProperty;
        }

        public bool IsGeoPropertyType(string field) {
            if (String.IsNullOrEmpty(field))
                return false;

            return GetMappingProperty(field, true) is IGeoPointProperty;
        }

        public bool IsNumericPropertyType(string field) {
            if (String.IsNullOrEmpty(field))
                return false;

            return GetMappingProperty(field, true) is INumberProperty;
        }

        public bool IsBooleanPropertyType(string field) {
            if (String.IsNullOrEmpty(field))
                return false;

            return GetMappingProperty(field, true) is IBooleanProperty;
        }

        public bool IsDatePropertyType(string field) {
            if (String.IsNullOrEmpty(field))
                return false;

            return GetMappingProperty(field, true) is IDateProperty;
        }

        public FieldType GetFieldType(string field) {
            if (String.IsNullOrWhiteSpace(field))
                return FieldType.None;

            var property = GetMappingProperty(field, true);

            if (property?.Type == null)
                return FieldType.None;

            switch (property.Type) {
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
                _lastMappingUpdate = DateTime.UtcNow;
                _logger.LogInformation("Got server mapping");

                return true;
            } catch (Exception ex) {
                _logger.LogError(ex, "Error getting server mapping: " + ex.Message);
                return false;
            }
        }

        public static ElasticMappingResolver Create<T>(Func<TypeMappingDescriptor<T>, ITypeMapping> mappingBuilder, IElasticClient client, ILogger logger = null) where T : class {
            logger = logger ?? NullLogger.Instance;

            return Create(mappingBuilder, client.Infer, () => {
                client.Indices.Refresh(Indices.Index<T>());
                var response = client.Indices.GetMapping(new GetMappingRequest(Indices.Index<T>()));
                logger.LogTrace("GetMapping: {Request}", response.GetRequest(false, true));

                // use first returned mapping because index could have been an index alias
                var mapping = response.Indices.Values.FirstOrDefault()?.Mappings;
                return mapping;
            }, logger);
        }

        public static ElasticMappingResolver Create<T>(Func<TypeMappingDescriptor<T>, ITypeMapping> mappingBuilder, IElasticClient client, string index, ILogger logger = null) where T : class {
            logger = logger ?? NullLogger.Instance;

            return Create(mappingBuilder, client.Infer, () => {
                client.Indices.Refresh(index);
                var response = client.Indices.GetMapping(new GetMappingRequest(index));
                logger.LogTrace("GetMapping: {Request}", response.GetRequest(false, true));

                // use first returned mapping because index could have been an index alias
                var mapping = response.Indices.Values.FirstOrDefault()?.Mappings;
                return mapping;
            }, logger);
        }

        public static ElasticMappingResolver Create<T>(Func<TypeMappingDescriptor<T>, ITypeMapping> mappingBuilder, Inferrer inferrer, Func<ITypeMapping> getMapping, ILogger logger = null) where T : class {
            var codeMapping = new TypeMappingDescriptor<T>();
            codeMapping = mappingBuilder(codeMapping) as TypeMappingDescriptor<T>;
            return new ElasticMappingResolver(codeMapping, inferrer, getMapping, logger: logger);
        }

        public static ElasticMappingResolver Create<T>(IElasticClient client, ILogger logger = null) {
            logger = logger ?? NullLogger.Instance;

            return Create(() => {
                client.Indices.Refresh(Indices.Index<T>());
                var response = client.Indices.GetMapping(new GetMappingRequest(Indices.Index<T>()));
                logger.LogTrace("GetMapping: {Request}", response.GetRequest(false, true));

                // use first returned mapping because index could have been an index alias
                var mapping = response.Indices.Values.FirstOrDefault()?.Mappings;
                return mapping;
            }, client.Infer, logger);
        }

        public static ElasticMappingResolver Create(IElasticClient client, string index, ILogger logger = null) {
            logger = logger ?? NullLogger.Instance;

            return Create(() => {
                client.Indices.Refresh(index);
                var response = client.Indices.GetMapping(new GetMappingRequest(index));
                logger.LogTrace("GetMapping: {Request}", response.GetRequest(false, true));

                // use first returned mapping because index could have been an index alias
                var mapping = response.Indices.Values.FirstOrDefault()?.Mappings;
                return mapping;
            }, client.Infer, logger);
        }

        public static ElasticMappingResolver Create(Func<ITypeMapping> getMapping, Inferrer inferrer, ILogger logger = null) {
            return new ElasticMappingResolver(getMapping, inferrer, logger: logger);
        }
    }

    public class FieldMapping {
        public FieldMapping(string path, IProperty property, DateTime? serverMapTime) {
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
}
