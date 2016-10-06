using System;
using System.Collections.Generic;
using System.Linq;
using Exceptionless.DateTimeExtensions;
using Foundatio.Parsers.ElasticQueries.Visitors;
using Foundatio.Parsers.LuceneQueries.Nodes;
using Foundatio.Parsers.LuceneQueries.Visitors;
using Nest;

namespace Foundatio.Parsers.ElasticQueries {
    public class ElasticQueryParserConfiguration {
        private readonly SortedSet<QueryVisitorWithPriority> _visitors = new SortedSet<QueryVisitorWithPriority>(new QueryVisitorWithPriority.PriorityComparer());

        public string DefaultField { get; private set; } = "_all";
        public Operator DefaultFilterOperator { get; private set; } = Operator.And;
        public Operator DefaultQueryOperator { get; private set; } = Operator.Or;
        public IList<IQueryNodeVisitorWithResult<IQueryNode>> Visitors => _visitors.Cast<IQueryNodeVisitorWithResult<IQueryNode>>().ToList();
        public RootObjectMapping Mapping { get; set; }
        private Func<RootObjectMapping> UpdateMappingFunc { get; set; }
        private DateTime? _lastMappingUpdate = null;

        public IElasticType GetFieldMapping(string field) {
            if (String.IsNullOrEmpty(field) || Mapping == null)
                return null;

            string[] fieldParts = field.Split('.');
            ObjectMapping currentObject = Mapping;

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
                        currentObject = Mapping;
                        continue;
                    }

                    if (fieldMapping == null)
                        return null;
                }

                if (depth == fieldParts.Length - 1)
                    return fieldMapping;

                if (fieldMapping is ObjectMapping)
                    currentObject = fieldMapping as ObjectMapping;
                else
                    return null;
            }

            return null;
        }

        private bool UpdateMapping() {
            if (_lastMappingUpdate.HasValue && _lastMappingUpdate.Value > DateTime.Now.SubtractMinutes(1))
                return false;

            Mapping = UpdateMappingFunc();
            _lastMappingUpdate = DateTime.Now;

            return true;
        }

        public bool IsFieldAnalyzed(string field) {
            if (String.IsNullOrEmpty(field))
                return true;

            var mapping = GetFieldMapping(field) as StringMapping;
            if (mapping == null)
                return false;

            return mapping.Index == FieldIndexOption.Analyzed || mapping.Index == null;
        }

        public bool IsNestedFieldType(string field) {
            if (String.IsNullOrEmpty(field))
                return false;

            var mapping = GetFieldMapping(field) as NestedObjectMapping;
            return mapping != null;
        }

        public bool IsGeoFieldType(string field) {
            if (String.IsNullOrEmpty(field))
                return false;

            var mapping = GetFieldMapping(field) as GeoPointMapping;
            return mapping != null;
        }

        public ElasticQueryParserConfiguration SetDefaultField(string field) {
            DefaultField = field;
            return this;
        }

        public ElasticQueryParserConfiguration SetDefaultFilterOperator(Operator op) {
            DefaultFilterOperator = op;
            return this;
        }

        public ElasticQueryParserConfiguration SetDefaultQueryOperator(Operator op) {
            DefaultQueryOperator = op;
            return this;
        }

        public ElasticQueryParserConfiguration UseMappings<T>(Func<PutMappingDescriptor<T>, PutMappingDescriptor<T>> mappingBuilder, Func<RootObjectMapping> getMapping) where T : class {
            var descriptor = new PutMappingDescriptor<T>(new ConnectionSettings());
            descriptor = mappingBuilder(descriptor);
            Mapping = ((IPutMappingRequest<T>)descriptor).Mapping;
            UpdateMappingFunc = getMapping;

            return this;
        }

        public ElasticQueryParserConfiguration UseMappings(Func<RootObjectMapping> getMapping) {
            Mapping = getMapping();
            UpdateMappingFunc = getMapping;

            return this;
        }

        public ElasticQueryParserConfiguration AddVisitor(IChainableQueryVisitor visitor, int priority = 0) {
            _visitors.Add(new QueryVisitorWithPriority { Visitor = visitor, Priority = priority });
            return this;
        }

        public ElasticQueryParserConfiguration UseAliases(AliasMap aliasMap, int priority = 0) {
            return AddVisitor(new AliasedQueryVisitor(aliasMap), priority);
        }

        public ElasticQueryParserConfiguration UseGeo(Func<string, string> resolveGeoLocation, int priority = 200) {
            return AddVisitor(new GeoVisitor(IsGeoFieldType, resolveGeoLocation), priority);
        }

        public ElasticQueryParserConfiguration UseNested(int priority = 300) {
            return AddVisitor(new NestedVisitor(IsNestedFieldType), priority);
        }
    }
}