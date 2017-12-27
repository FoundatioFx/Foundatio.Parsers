using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using Exceptionless.DateTimeExtensions;
using Foundatio.Parsers.ElasticQueries.Visitors;
using Foundatio.Parsers.LuceneQueries.Visitors;
using Nest;

namespace Foundatio.Parsers.ElasticQueries {
    public class ElasticQueryParserConfiguration {
        private ITypeMapping _serverMapping;
        private ITypeMapping _codeMapping;

        public ElasticQueryParserConfiguration() {
            AddQueryVisitor(new CombineQueriesVisitor(), 10000);
            AddSortVisitor(new TermToFieldVisitor(), 0);
            AddAggregationVisitor(new AssignAggregationTypeVisitor(), 0);
            AddAggregationVisitor(new CombineAggregationsVisitor(), 10000);
        }

        public string[] DefaultFields { get; private set; } = new[] { "_all" };
        public AliasResolver DefaultAliasResolver { get; private set; }
        public Func<string, Task<string>> DefaultIncludeResolver { get; private set; }
        public Func<QueryValidationInfo, Task<bool>> DefaultValidator { get; private set; }
        public ChainedQueryVisitor SortVisitor { get; } = new ChainedQueryVisitor();
        public ChainedQueryVisitor QueryVisitor { get; } = new ChainedQueryVisitor();
        public ChainedQueryVisitor AggregationVisitor { get; } = new ChainedQueryVisitor();

        public ElasticQueryParserConfiguration SetDefaultFields(string[] fields) {
            DefaultFields = fields;
            return this;
        }

        public ElasticQueryParserConfiguration UseAliases(int priority = 50) {
            return UseAliases((AliasResolver)null, priority);
        }

        public ElasticQueryParserConfiguration UseAliases(AliasResolver defaultAliasResolver, int priority = 50) {
            DefaultAliasResolver = defaultAliasResolver;

            var visitor = new AliasedQueryVisitor();
            QueryVisitor.AddVisitor(visitor, priority);
            SortVisitor.AddVisitor(visitor, priority);

            AggregationVisitor.AddVisitor(new AliasedQueryVisitor(false), priority);
            return this;
        }

        public ElasticQueryParserConfiguration UseAliases(AliasMap defaultAliasMap, int priority = 50) {
            return UseAliases(defaultAliasMap.Resolve, priority);
        }

        public ElasticQueryParserConfiguration UseGeo(Func<string, string> resolveGeoLocation, int priority = 200) {
            return UseGeo(location => Task.FromResult(resolveGeoLocation(location)), priority);
        }

        public ElasticQueryParserConfiguration UseGeo(Func<string, Task<string>> resolveGeoLocation, int priority = 200) {
            return AddVisitor(new GeoVisitor(resolveGeoLocation), priority);
        }

        public ElasticQueryParserConfiguration UseIncludes(Func<string, Task<string>> includeResolver, int priority = 0) {
            DefaultIncludeResolver = includeResolver;

            return AddVisitor(new IncludeVisitor(), priority);
        }

        public ElasticQueryParserConfiguration UseValidation(Func<QueryValidationInfo, Task<bool>> validator, int priority = 0) {
            DefaultValidator = validator;

            return AddVisitor(new ValidationVisitor { ShouldThrow = true }, priority);
        }

        public ElasticQueryParserConfiguration UseIncludes(Func<string, string> resolveInclude, int priority = 0) {
            return UseIncludes(name => Task.FromResult(resolveInclude(name)), priority);
        }

        public ElasticQueryParserConfiguration UseIncludes(IDictionary<string, string> includes, int priority = 0) {
            return UseIncludes(name => includes.ContainsKey(name) ? includes[name] : null, priority);
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
            if (_serverMapping == null)
                GetServerMapping();

            if (String.IsNullOrEmpty(field) || _serverMapping == null)
                return null;

            var fieldParts = field.Split('.');
            var currentProperties = MergeProperties(_codeMapping?.Properties, _serverMapping?.Properties);

            for (int depth = 0; depth < fieldParts.Length; depth++) {
                string fieldPart = fieldParts[depth];
                IProperty fieldMapping = null;
                if (currentProperties == null || !currentProperties.TryGetValue(fieldPart, out fieldMapping)) {
                    // check to see if there is an index_name match
                    if (currentProperties != null)
                        fieldMapping = currentProperties
                            .Select(m => m.Value)
                            .FirstOrDefault(m => m.Name == fieldPart);

                    if (fieldMapping == null && GetServerMapping()) {
                        // we have updated mapping, start over from the top
                        depth = -1;
                        currentProperties = MergeProperties(_codeMapping.Properties, _serverMapping.Properties);
                        continue;
                    }

                    if (fieldMapping == null)
                        return null;
                }

                if (depth == fieldParts.Length - 1)
                    return fieldMapping;

                if (fieldMapping is IObjectProperty objectProperty)
                    currentProperties = objectProperty.Properties;
                else {
                    if (fieldMapping is ITextProperty textProperty)
                        currentProperties = textProperty.Fields;
                    else
                        return null;
                }
            }

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
                _lastMappingUpdate = DateTime.UtcNow;

                return true;
            } catch (Exception) {
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
            return UseMappings(mappingBuilder, () => client.GetMapping(new GetMappingRequest(index, Types.Type<T>())).Mapping);
        }

        public ElasticQueryParserConfiguration UseMappings<T>(Func<TypeMappingDescriptor<T>, TypeMappingDescriptor<T>> mappingBuilder, Func<ITypeMapping> getMapping) where T : class {
            var descriptor = new TypeMappingDescriptor<T>();
            descriptor = mappingBuilder(descriptor);
            _codeMapping = descriptor;
            GetServerMappingFunc = getMapping;

            return this;
        }

        public ElasticQueryParserConfiguration UseMappings<T>(IElasticClient client) {
            return UseMappings(() => client.GetMapping(new GetMappingRequest(Indices.Index<T>(), Types.Type<T>())).Mapping);
        }

        public ElasticQueryParserConfiguration UseMappings<T>(IElasticClient client, string index) {
            return UseMappings(() => client.GetMapping(new GetMappingRequest(index, Types.Type<T>())).Mapping);
        }

        public ElasticQueryParserConfiguration UseMappings(IElasticClient client, string index, string type) {
            return UseMappings(() => client.GetMapping(new GetMappingRequest(index, type)).Mapping);
        }

        public ElasticQueryParserConfiguration UseMappings(Func<ITypeMapping> getMapping) {
            GetServerMappingFunc = getMapping;

            return this;
        }
    }
}