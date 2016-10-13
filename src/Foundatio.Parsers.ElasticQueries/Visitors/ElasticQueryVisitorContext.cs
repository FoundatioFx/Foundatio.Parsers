using System;
using Foundatio.Parsers.LuceneQueries.Visitors;
using Nest;

namespace Foundatio.Parsers.ElasticQueries.Visitors {
    public class ElasticQueryVisitorContext : QueryVisitorContextWithAliasResolver, IElasticQueryVisitorContext {
        public Operator DefaultOperator { get; set; }
        public string DefaultField { get; set; }
        public Func<string, IElasticType> GetFieldMappingFunc { get; set; }

        public IElasticType GetFieldMapping(string field) {
            return GetFieldMappingFunc?.Invoke(field);
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
    }
}