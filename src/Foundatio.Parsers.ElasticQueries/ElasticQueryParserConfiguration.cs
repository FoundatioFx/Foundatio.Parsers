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
        public IList<IQueryNodeVisitorWithResult<IQueryNode>> Visitors => _visitors.Cast<IQueryNodeVisitorWithResult<IQueryNode>>().ToList();
        public ITypeMapping Mapping { get; set; }
        private Func<ITypeMapping> UpdateMappingFunc { get; set; }
        private DateTime? _lastMappingUpdate = null;

        public IProperty GetMappingProperty(string field) {
            if (String.IsNullOrEmpty(field) || Mapping == null)
                return null;

            string[] fieldParts = field.Split('.');
            IProperties currentProperties = Mapping.Properties;

            for (int depth = 0; depth < fieldParts.Length; depth++) {
                string fieldPart = fieldParts[depth];
                IProperty fieldMapping = null;
                if (currentProperties == null || !currentProperties.TryGetValue(fieldPart, out fieldMapping)) {
                    // check to see if there is an index_name match
                    if (currentProperties != null)
                        fieldMapping = currentProperties
                            .Select(m => m.Value)
                            .FirstOrDefault(m => m.IndexName == fieldPart);

                    if (fieldMapping == null && UpdateMapping()) {
                        // we have updated mapping, start over from the top
                        depth = -1;
                        currentProperties = Mapping.Properties;
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

        private bool UpdateMapping() {
            if (_lastMappingUpdate.HasValue && _lastMappingUpdate.Value > DateTime.Now.SubtractMinutes(1))
                return false;

            Mapping = UpdateMappingFunc();
            _lastMappingUpdate = DateTime.Now;

            return true;
        }

        public bool IsAnalyzedPropertyType(string field) {
            if (String.IsNullOrEmpty(field))
                return true;

            var mapping = GetMappingProperty(field) as TextProperty;
            if (mapping == null)
                return false;

            return !mapping.Index.HasValue || mapping.Index.Value;
        }

        public bool IsNestedPropertyType(string field) {
            if (String.IsNullOrEmpty(field))
                return false;

            var mapping = GetMappingProperty(field) as NestedProperty;
            return mapping != null;
        }

        public bool IsGeoPropertyType(string field) {
            if (String.IsNullOrEmpty(field))
                return false;

            var mapping = GetMappingProperty(field) as GeoPointProperty;
            return mapping != null;
        }

        public bool IsNumericPropertyType(string field) {
            if (String.IsNullOrEmpty(field))
                return false;

            var mapping = GetMappingProperty(field) as NumberProperty;
            return mapping != null;
        }

        public ElasticQueryParserConfiguration SetDefaultField(string field) {
            DefaultField = field;
            return this;
        }

        public ElasticQueryParserConfiguration UseMappings<T>(Func<TypeMappingDescriptor<T>, TypeMappingDescriptor<T>> mappingBuilder, Func<ITypeMapping> getMapping) where T : class {
            var descriptor = new TypeMappingDescriptor<T>();
            descriptor = mappingBuilder(descriptor);
            Mapping = descriptor;
            UpdateMappingFunc = getMapping;

            return this;
        }

        public ElasticQueryParserConfiguration UseMappings(Func<ITypeMapping> getMapping) {
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
            return AddVisitor(new GeoVisitor(IsGeoPropertyType, resolveGeoLocation), priority);
        }

        public ElasticQueryParserConfiguration UseNested(int priority = 1000) {
            return AddVisitor(new NestedVisitor(IsNestedPropertyType), priority);
        }
    }
}