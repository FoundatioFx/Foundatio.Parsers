using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using Exceptionless.DateTimeExtensions;
using Foundatio.Parsers.ElasticQueries.Extensions;
using Foundatio.Parsers.ElasticQueries.Visitors;
using Foundatio.Parsers.LuceneQueries.Visitors;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Logging.Abstractions;
using Nest;

namespace Foundatio.Parsers.ElasticQueries {
    public class ElasticQueryParserConfiguration {
        private ITypeMapping _serverMapping;
        private ITypeMapping _codeMapping;
        private readonly ConcurrentDictionary<string, IProperty> _propertyCache = new ConcurrentDictionary<string, IProperty>();
        private ILogger _logger = NullLogger.Instance;

        public ElasticQueryParserConfiguration() {
            AddQueryVisitor(new CombineQueriesVisitor(), 10000);
            AddSortVisitor(new TermToFieldVisitor(), 0);
            AddAggregationVisitor(new AssignOperationTypeVisitor(), 0);
            AddAggregationVisitor(new CombineAggregationsVisitor(), 10000);
            AddVisitor(new FieldResolverQueryVisitor(), 10);
        }

        public ILoggerFactory LoggerFactory { get; private set; } = NullLoggerFactory.Instance;
        public string[] DefaultFields { get; private set; }
        public QueryFieldResolver FieldResolver { get; private set; }
        public IncludeResolver IncludeResolver { get; private set; }
        public Func<QueryValidationInfo, Task<bool>> Validator { get; private set; }
        public ChainedQueryVisitor SortVisitor { get; } = new ChainedQueryVisitor();
        public ChainedQueryVisitor QueryVisitor { get; } = new ChainedQueryVisitor();
        public ChainedQueryVisitor AggregationVisitor { get; } = new ChainedQueryVisitor();

        public ElasticQueryParserConfiguration SetLoggerFactory(ILoggerFactory loggerFactory) {
            LoggerFactory = loggerFactory ?? NullLoggerFactory.Instance;
            _logger = loggerFactory.CreateLogger<ElasticQueryParserConfiguration>();
            
            return this;
        }

        public ElasticQueryParserConfiguration SetDefaultFields(string[] fields) {
            DefaultFields = fields;
            return this;
        }

        public ElasticQueryParserConfiguration UseFieldResolver(QueryFieldResolver resolver, int priority = 10) {
            FieldResolver = resolver;

            ReplaceVisitor<FieldResolverQueryVisitor>(new FieldResolverQueryVisitor(resolver), priority);

            return this;
        }

        public ElasticQueryParserConfiguration UseFieldMap(IDictionary<string, string> fields, int priority = 10) {
            if (fields != null)
                return UseFieldResolver(fields.ToHierarchicalFieldResolver(), priority);
            else
                return UseFieldResolver(null);
        }

        public ElasticQueryParserConfiguration UseGeo(Func<string, string> resolveGeoLocation, int priority = 200) {
            return UseGeo(location => Task.FromResult(resolveGeoLocation(location)), priority);
        }

        public ElasticQueryParserConfiguration UseGeo(Func<string, Task<string>> resolveGeoLocation, int priority = 200) {
            return AddVisitor(new GeoVisitor(resolveGeoLocation), priority);
        }

        public ElasticQueryParserConfiguration UseIncludes(IncludeResolver includeResolver, ShouldSkipIncludeFunc shouldSkipInclude = null, int priority = 0) {
            IncludeResolver = includeResolver;

            return AddVisitor(new IncludeVisitor(shouldSkipInclude), priority);
        }

        public ElasticQueryParserConfiguration UseIncludes(Func<string, string> resolveInclude, ShouldSkipIncludeFunc shouldSkipInclude = null, int priority = 0) {
            return UseIncludes(name => Task.FromResult(resolveInclude(name)), shouldSkipInclude, priority);
        }

        public ElasticQueryParserConfiguration UseIncludes(IDictionary<string, string> includes, ShouldSkipIncludeFunc shouldSkipInclude = null, int priority = 0) {
            return UseIncludes(name => includes.ContainsKey(name) ? includes[name] : null, shouldSkipInclude, priority);
        }

        public ElasticQueryParserConfiguration UseValidation(Func<QueryValidationInfo, Task<bool>> validator, int priority = 0) {
            Validator = validator;

            return AddVisitor(new ValidationVisitor { ShouldThrow = true }, priority);
        }

        public ElasticQueryParserConfiguration UseNested(int priority = 300) {
            return AddVisitor(new NestedVisitor(), priority);
        }

        #region Combined Visitor Management

        public ElasticQueryParserConfiguration AddVisitor(IChainableQueryVisitor visitor, int priority = 0) {
            QueryVisitor.AddVisitor(visitor, priority);
            AggregationVisitor.AddVisitor(visitor, priority);
            SortVisitor.AddVisitor(visitor, priority);

            return this;
        }

        public ElasticQueryParserConfiguration RemoveVisitor<T>() where T : IChainableQueryVisitor {
            QueryVisitor.RemoveVisitor<T>();
            AggregationVisitor.RemoveVisitor<T>();
            SortVisitor.RemoveVisitor<T>();

            return this;
        }

        public ElasticQueryParserConfiguration ReplaceVisitor<T>(IChainableQueryVisitor visitor, int? newPriority = null) where T : IChainableQueryVisitor {
            QueryVisitor.ReplaceVisitor<T>(visitor, newPriority);
            AggregationVisitor.ReplaceVisitor<T>(visitor, newPriority);
            SortVisitor.ReplaceVisitor<T>(visitor, newPriority);

            return this;
        }

        public ElasticQueryParserConfiguration AddVisitorBefore<T>(IChainableQueryVisitor visitor) {
            QueryVisitor.AddVisitorBefore<T>(visitor);
            AggregationVisitor.AddVisitorBefore<T>(visitor);
            SortVisitor.AddVisitorBefore<T>(visitor);

            return this;
        }

        public ElasticQueryParserConfiguration AddVisitorAfter<T>(IChainableQueryVisitor visitor) {
            QueryVisitor.AddVisitorAfter<T>(visitor);
            AggregationVisitor.AddVisitorAfter<T>(visitor);
            SortVisitor.AddVisitorAfter<T>(visitor);

            return this;
        }

        #endregion

        #region Query Visitor Management

        public ElasticQueryParserConfiguration AddQueryVisitor(IChainableQueryVisitor visitor, int priority = 0) {
            QueryVisitor.AddVisitor(visitor, priority);

            return this;
        }

        public ElasticQueryParserConfiguration RemoveQueryVisitor<T>() where T : IChainableQueryVisitor {
            QueryVisitor.RemoveVisitor<T>();

            return this;
        }

        public ElasticQueryParserConfiguration ReplaceQueryVisitor<T>(IChainableQueryVisitor visitor, int? newPriority = null) where T : IChainableQueryVisitor {
            QueryVisitor.ReplaceVisitor<T>(visitor, newPriority);

            return this;
        }

        public ElasticQueryParserConfiguration AddQueryVisitorBefore<T>(IChainableQueryVisitor visitor) {
            QueryVisitor.AddVisitorBefore<T>(visitor);

            return this;
        }

        public ElasticQueryParserConfiguration AddQueryVisitorAfter<T>(IChainableQueryVisitor visitor) {
            QueryVisitor.AddVisitorAfter<T>(visitor);

            return this;
        }

        #endregion

        #region Sort Visitor Management

        public ElasticQueryParserConfiguration AddSortVisitor(IChainableQueryVisitor visitor, int priority = 0)
        {
            SortVisitor.AddVisitor(visitor, priority);

            return this;
        }

        public ElasticQueryParserConfiguration RemoveSortVisitor<T>() where T : IChainableQueryVisitor
        {
            SortVisitor.RemoveVisitor<T>();

            return this;
        }

        public ElasticQueryParserConfiguration ReplaceSortVisitor<T>(IChainableQueryVisitor visitor, int? newPriority = null) where T : IChainableQueryVisitor
        {
            SortVisitor.ReplaceVisitor<T>(visitor, newPriority);

            return this;
        }

        public ElasticQueryParserConfiguration AddSortVisitorBefore<T>(IChainableQueryVisitor visitor)
        {
            SortVisitor.AddVisitorBefore<T>(visitor);

            return this;
        }

        public ElasticQueryParserConfiguration AddSortVisitorAfter<T>(IChainableQueryVisitor visitor)
        {
            SortVisitor.AddVisitorAfter<T>(visitor);

            return this;
        }

        #endregion

        #region Aggregation Visitor Management

        public ElasticQueryParserConfiguration AddAggregationVisitor(IChainableQueryVisitor visitor, int priority = 0) {
            AggregationVisitor.AddVisitor(visitor, priority);

            return this;
        }

        public ElasticQueryParserConfiguration RemoveAggregationVisitor<T>() where T : IChainableQueryVisitor {
            AggregationVisitor.RemoveVisitor<T>();

            return this;
        }

        public ElasticQueryParserConfiguration ReplaceAggregationVisitor<T>(IChainableQueryVisitor visitor, int? newPriority = null) where T : IChainableQueryVisitor {
            AggregationVisitor.ReplaceVisitor<T>(visitor, newPriority);

            return this;
        }

        public ElasticQueryParserConfiguration AddAggregationVisitorBefore<T>(IChainableQueryVisitor visitor) {
            AggregationVisitor.AddVisitorBefore<T>(visitor);

            return this;
        }

        public ElasticQueryParserConfiguration AddAggregationVisitorAfter<T>(IChainableQueryVisitor visitor) {
            AggregationVisitor.AddVisitorAfter<T>(visitor);

            return this;
        }

        #endregion

        public IProperty GetMappingProperty(string field) {
            if (String.IsNullOrEmpty(field))
                return null;
            
            if (_propertyCache.TryGetValue(field, out var propertyType)) {
                _logger.LogTrace("Cached property mapping: {field}={type}", field, propertyType);
                return propertyType;
            }
            
            if (_serverMapping == null)
                GetServerMapping();

            if (_serverMapping == null) {
                _logger.LogTrace("Server mapping is null");
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
                        fieldMapping = currentProperties
                            .Select(m => m.Value)
                            .FirstOrDefault(m => m.Name == fieldPart);
                    
                    // no mapping found, call GetServerMapping again in case it hasn't been called recently and there are possibly new mappings
                    if (fieldMapping == null && GetServerMapping()) {
                        // we got updated mapping, start over from the top
                        depth = -1;
                        currentProperties = MergeProperties(_codeMapping.Properties, _serverMapping.Properties);
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

        private IProperties MergeProperties(IProperties codeProperties, IProperties serverProperties) {
            if (codeProperties == null || serverProperties == null)
                return codeProperties ?? serverProperties;

            IProperties properties = new Properties();
            foreach (var serverProperty in serverProperties) {
                var merged = serverProperty.Value;
                if (codeProperties.TryGetValue(serverProperty.Key, out var codeProperty))
                    merged.LocalMetadata = codeProperty.LocalMetadata;

                switch (merged) {
                    case IObjectProperty objectProperty:
                        var codeObjectProperty = codeProperty as IObjectProperty;
                        objectProperty.Properties =
                            MergeProperties(codeObjectProperty?.Properties, objectProperty.Properties);
                        break;
                    case ITextProperty textProperty:
                        var codeTextProperty = codeProperty as ITextProperty;
                        textProperty.Fields = MergeProperties(codeTextProperty?.Fields, textProperty.Fields);
                        break;
                }

                properties.Add(serverProperty.Key, merged);
            }

            foreach (var codeProperty in codeProperties) {
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

        /// <summary>
        /// Allows you to refresh server side mapping. This should be used only in unit tests.
        /// </summary>
        public void RefreshMapping() {
            _serverMapping = null;
            _lastMappingUpdate = null;
        }

        public ElasticQueryParserConfiguration UseMappings<T>(Func<TypeMappingDescriptor<T>, TypeMappingDescriptor<T>> mappingBuilder, IElasticClient client, string index) where T : class {
            return UseMappings(mappingBuilder, () => {
                var response = client.Indices.GetMapping(new GetMappingRequest(index));
                _logger.LogTrace("GetMapping: {Request}", response.GetRequest(false, true));
                return response.GetMappingFor(index);
            });
        }

        public ElasticQueryParserConfiguration UseMappings<T>(Func<TypeMappingDescriptor<T>, TypeMappingDescriptor<T>> mappingBuilder, Func<ITypeMapping> getMapping) where T : class {
            var descriptor = new TypeMappingDescriptor<T>();
            descriptor = mappingBuilder(descriptor);
            _codeMapping = descriptor;
            GetServerMappingFunc = getMapping;

            return this;
        }

        public ElasticQueryParserConfiguration UseMappings<T>(IElasticClient client) {
            return UseMappings(() => {
                var response = client.Indices.GetMapping(new GetMappingRequest(Indices.Index<T>()));
                _logger.LogTrace("GetMapping: {Request}", response.GetRequest(false, true));
                return response.GetMappingFor(Indices.Index<T>());
            });
        }

        public ElasticQueryParserConfiguration UseMappings(IElasticClient client, string index) {
            return UseMappings(() => {
                var response = client.Indices.GetMapping(new GetMappingRequest(index));
                _logger.LogTrace("GetMapping: {Request}", response.GetRequest(false, true));
                
                // use first returned mapping because index could have been an index alias
                var mapping = response.Indices.Values.FirstOrDefault()?.Mappings;
                return mapping;
            });
        }

        public ElasticQueryParserConfiguration UseMappings(Func<ITypeMapping> getMapping) {
            GetServerMappingFunc = getMapping;

            return this;
        }
    }
}