using System;
using System.Linq;
using Exceptionless.DateTimeExtensions;
using Foundatio.Parsers.ElasticQueries.Visitors;
using Foundatio.Parsers.LuceneQueries.Visitors;
using Nest;

namespace Foundatio.Parsers.ElasticQueries {
    public class ElasticQueryParserConfiguration {
        private ITypeMapping _mapping;

        public ElasticQueryParserConfiguration() {
            AddQueryVisitor(new CombineQueriesVisitor(), 10000);
            AddAggregationVisitor(new AssignAggregationTypeVisitor(), 0);
            AddAggregationVisitor(new CombineAggregationsVisitor(), 10000);
        }

        public string DefaultField { get; private set; } = "_all";
        public AliasResolver DefaultAliasResolver { get; private set; }
        public ChainedQueryVisitor QueryVisitor { get; } = new ChainedQueryVisitor();
        public ChainedQueryVisitor AggregationVisitor { get; } = new ChainedQueryVisitor();

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
            QueryVisitor.AddVisitor(visitor, priority);
            AggregationVisitor.AddVisitor(visitor, priority);

            return this;
        }

        public ElasticQueryParserConfiguration RemoveVisitor<T>() where T : IChainableQueryVisitor {
            QueryVisitor.RemoveVisitor<T>();
            AggregationVisitor.RemoveVisitor<T>();

            return this;
        }

        public ElasticQueryParserConfiguration ReplaceVisitor<T>(IChainableQueryVisitor visitor, int? newPriority = null) where T : IChainableQueryVisitor {
            QueryVisitor.ReplaceVisitor<T>(visitor, newPriority);
            AggregationVisitor.ReplaceVisitor<T>(visitor, newPriority);

            return this;
        }

        public ElasticQueryParserConfiguration AddVisitorBefore<T>(IChainableQueryVisitor visitor) {
            QueryVisitor.AddVisitorBefore<T>(visitor);
            AggregationVisitor.AddVisitorBefore<T>(visitor);

            return this;
        }

        public ElasticQueryParserConfiguration AddVisitorAfter<T>(IChainableQueryVisitor visitor) {
            QueryVisitor.AddVisitorAfter<T>(visitor);
            AggregationVisitor.AddVisitorAfter<T>(visitor);

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

        public IProperty GetMappingProperty(string field) {
            if (String.IsNullOrEmpty(field) || _mapping == null)
                return null;

            string[] fieldParts = field.Split('.');
            IProperties currentProperties = _mapping.Properties;

            for (int depth = 0; depth < fieldParts.Length; depth++) {
                string fieldPart = fieldParts[depth];
                IProperty fieldMapping = null;
                if (currentProperties == null || !currentProperties.TryGetValue(fieldPart, out fieldMapping)) {
                    // check to see if there is an index_name match
                    if (currentProperties != null)
                        fieldMapping = currentProperties
                            .Select(m => m.Value)
                            .FirstOrDefault(m => m.Name == fieldPart);

                    if (fieldMapping == null && UpdateMapping()) {
                        // we have updated mapping, start over from the top
                        depth = -1;
                        currentProperties = _mapping.Properties;
                        continue;
                    }

                    if (fieldMapping == null)
                        return null;
                }

                if (depth == fieldParts.Length - 1)
                    return fieldMapping;

                var objectProperty = fieldMapping as ObjectProperty;
                if (objectProperty != null)
                    currentProperties = objectProperty.Properties;
                else
                    return null;
            }

            return null;
        }

        private Func<ITypeMapping> UpdateMappingFunc { get; set; }
        private DateTime? _lastMappingUpdate = null;
        private bool UpdateMapping() {
            if (_lastMappingUpdate.HasValue && _lastMappingUpdate.Value > DateTime.Now.SubtractMinutes(1))
                return false;

            _mapping = UpdateMappingFunc();
            _lastMappingUpdate = DateTime.Now;

            return true;
        }

        public ElasticQueryParserConfiguration UseMappings<T>(Func<TypeMappingDescriptor<T>, TypeMappingDescriptor<T>> mappingBuilder, Func<ITypeMapping> getMapping) where T : class {
            var descriptor = new TypeMappingDescriptor<T>();
            descriptor = mappingBuilder(descriptor);
            _mapping = descriptor;
            UpdateMappingFunc = getMapping;

            return this;
        }

        public ElasticQueryParserConfiguration UseMappings(Func<ITypeMapping> getMapping) {
            _mapping = getMapping();
            UpdateMappingFunc = getMapping;

            return this;
        }
    }
}