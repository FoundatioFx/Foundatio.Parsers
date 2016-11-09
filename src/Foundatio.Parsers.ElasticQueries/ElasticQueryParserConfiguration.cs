using System;
using System.Linq;
using Exceptionless.DateTimeExtensions;
using Foundatio.Parsers.ElasticQueries.Visitors;
using Foundatio.Parsers.LuceneQueries.Visitors;
using Nest;

namespace Foundatio.Parsers.ElasticQueries {
    public class ElasticQueryParserConfiguration {
        private RootObjectMapping _mapping;

        public ElasticQueryParserConfiguration() {
            AddFilterVisitor(new CombineFiltersVisitor(), 10000);
            AddQueryVisitor(new CombineQueriesVisitor(), 10000);
            AddAggregationVisitor(new AssignAggregationTypeVisitor(), 0);
            AddAggregationVisitor(new CombineAggregationsVisitor(), 10000);
        }

        public string DefaultField { get; private set; } = "_all";
        public AliasResolver DefaultAliasResolver { get; private set; }
        public ChainedQueryVisitor FilterVisitor { get; } = new ChainedQueryVisitor();
        public ChainedQueryVisitor QueryVisitor { get; } = new ChainedQueryVisitor();
        public ChainedQueryVisitor AggregationVisitor { get; } = new ChainedQueryVisitor();

        public ElasticQueryParserConfiguration UseMappings<T>(Func<PutMappingDescriptor<T>, PutMappingDescriptor<T>> mappingBuilder, Func<RootObjectMapping> getMapping) where T : class {
            var descriptor = new PutMappingDescriptor<T>(new ConnectionSettings());
            descriptor = mappingBuilder(descriptor);
            _mapping = ((IPutMappingRequest<T>)descriptor).Mapping;
            UpdateMappingFunc = getMapping;

            return this;
        }

        public ElasticQueryParserConfiguration UseMappings<T>(IElasticClient client) {
            return UseMappings(() => client.GetMapping(new GetMappingRequest(null, typeof(T))).Mapping);
        }

        public ElasticQueryParserConfiguration UseMappings<T>(IElasticClient client, string index) {
            return UseMappings(() => client.GetMapping(new GetMappingRequest(index, typeof(T))).Mapping);
        }

        public ElasticQueryParserConfiguration UseMappings(IElasticClient client, string index, string type) {
            return UseMappings(() => client.GetMapping(new GetMappingRequest(index, type)).Mapping);
        }

        public ElasticQueryParserConfiguration UseMappings(Func<RootObjectMapping> getMapping) {
            _mapping = getMapping();
            UpdateMappingFunc = getMapping;
            
            return this;
        }

        public ElasticQueryParserConfiguration UseAliases(int priority = 50) {
            return AddVisitor(new AliasedQueryVisitor(), priority);
        }

        public ElasticQueryParserConfiguration UseAliases(AliasResolver defaultAliasResolver, int priority = 50) {
            DefaultAliasResolver = defaultAliasResolver;

            return AddVisitor(new AliasedQueryVisitor(), priority);
        }

        public ElasticQueryParserConfiguration UseAliases(AliasMap defaultAliasMap, int priority = 50) {
            DefaultAliasResolver = defaultAliasMap.Resolve;

            return AddVisitor(new AliasedQueryVisitor(), priority);
        }

        public ElasticQueryParserConfiguration UseGeo(Func<string, string> resolveGeoLocation, int priority = 200) {
            return AddVisitor(new GeoVisitor(resolveGeoLocation), priority);
        }

        public ElasticQueryParserConfiguration UseNested(int priority = 300) {
            return AddVisitor(new NestedVisitor(), priority);
        }

        #region Combined Visitor Management

        public ElasticQueryParserConfiguration AddVisitor(IChainableQueryVisitor visitor, int priority = 0) {
            FilterVisitor.AddVisitor(visitor, priority);
            QueryVisitor.AddVisitor(visitor, priority);
            AggregationVisitor.AddVisitor(visitor, priority);

            return this;
        }

        public ElasticQueryParserConfiguration RemoveVisitor<T>() where T : IChainableQueryVisitor {
            FilterVisitor.RemoveVisitor<T>();
            QueryVisitor.RemoveVisitor<T>();
            AggregationVisitor.RemoveVisitor<T>();

            return this;
        }

        public ElasticQueryParserConfiguration ReplaceVisitor<T>(IChainableQueryVisitor visitor, int? newPriority = null) where T : IChainableQueryVisitor {
            FilterVisitor.ReplaceVisitor<T>(visitor, newPriority);
            QueryVisitor.ReplaceVisitor<T>(visitor, newPriority);
            AggregationVisitor.ReplaceVisitor<T>(visitor, newPriority);

            return this;
        }

        public ElasticQueryParserConfiguration AddVisitorBefore<T>(IChainableQueryVisitor visitor) {
            FilterVisitor.AddVisitorBefore<T>(visitor);
            QueryVisitor.AddVisitorBefore<T>(visitor);
            AggregationVisitor.AddVisitorBefore<T>(visitor);

            return this;
        }

        public ElasticQueryParserConfiguration AddVisitorAfter<T>(IChainableQueryVisitor visitor) {
            FilterVisitor.AddVisitorAfter<T>(visitor);
            QueryVisitor.AddVisitorAfter<T>(visitor);
            AggregationVisitor.AddVisitorAfter<T>(visitor);

            return this;
        }

        #endregion

        #region Filter Visitor Management

        public ElasticQueryParserConfiguration AddFilterVisitor(IChainableQueryVisitor visitor, int priority = 0) {
            FilterVisitor.AddVisitor(visitor, priority);

            return this;
        }

        public ElasticQueryParserConfiguration RemoveFilterVisitor<T>() where T : IChainableQueryVisitor {
            FilterVisitor.RemoveVisitor<T>();

            return this;
        }

        public ElasticQueryParserConfiguration ReplaceFilterVisitor<T>(IChainableQueryVisitor visitor, int? newPriority = null) where T : IChainableQueryVisitor {
            FilterVisitor.ReplaceVisitor<T>(visitor, newPriority);

            return this;
        }

        public ElasticQueryParserConfiguration AddFilterVisitorBefore<T>(IChainableQueryVisitor visitor) {
            FilterVisitor.AddVisitorBefore<T>(visitor);

            return this;
        }

        public ElasticQueryParserConfiguration AddFilterVisitorAfter<T>(IChainableQueryVisitor visitor) {
            FilterVisitor.AddVisitorAfter<T>(visitor);

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

        public IElasticType GetFieldMapping(string field) {
            if (String.IsNullOrEmpty(field) || _mapping == null)
                return null;

            string[] fieldParts = field.Split('.');
            ObjectMapping currentObject = _mapping;

            for (int depth = 0; depth < fieldParts.Length; depth++) {
                string fieldPart = fieldParts[depth];
                IElasticType fieldMapping = null;
                if (currentObject.Properties == null || !currentObject.Properties.TryGetValue(fieldPart, out fieldMapping)) {
                    // check to see if there is an index_name match
                    if (currentObject.Properties != null)
                        fieldMapping = currentObject.Properties.Values
                            .OfType<IElasticCoreType>()
                            .FirstOrDefault(m => m.IndexName == fieldPart);

                    if (fieldMapping == null && UpdateMapping()) {
                        // we have updated mapping, start over from the top
                        depth = -1;
                        currentObject = _mapping;
                        continue;
                    }

                    if (fieldMapping == null)
                        break;
                }

                if (depth == fieldParts.Length - 1)
                    return fieldMapping;

                if (fieldMapping is ObjectMapping)
                    currentObject = fieldMapping as ObjectMapping;
                else
                    break;
            }

            return GetRootFieldMapping(field, _mapping, true);
        }

        private IElasticType GetRootFieldMapping(string field, ObjectMapping mapping, bool isRoot) {
            if (mapping?.Properties == null)
                return null;

            bool justName = mapping.Path == "just_name";
            
            foreach (var property in mapping.Properties) {
                var elasticCoreType = property.Value as IElasticCoreType;
                if ((isRoot || justName) && elasticCoreType != null && (property.Key.Equals(field) || elasticCoreType.IndexName == field))
                    return property.Value;

                var objectProperty = property.Value as ObjectMapping;
                if (objectProperty == null)
                    continue;

                var result = GetRootFieldMapping(field, objectProperty, false);
                if (result != null)
                    return result;
            }

            return null;
        }

        private Func<RootObjectMapping> UpdateMappingFunc { get; set; }
        private DateTime? _lastMappingUpdate = null;
        private bool UpdateMapping() {
            if (_lastMappingUpdate.HasValue && _lastMappingUpdate.Value > DateTime.Now.SubtractMinutes(1))
                return false;

            _mapping = UpdateMappingFunc();
            _lastMappingUpdate = DateTime.Now;

            return true;
        }
    }
}