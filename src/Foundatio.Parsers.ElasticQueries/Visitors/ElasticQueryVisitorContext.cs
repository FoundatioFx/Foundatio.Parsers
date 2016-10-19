using System;
using Foundatio.Parsers.LuceneQueries.Visitors;
using Nest;

namespace Foundatio.Parsers.ElasticQueries.Visitors {
    public class ElasticQueryVisitorContext : QueryVisitorContextWithAliasResolver, IElasticQueryVisitorContext {
        public Operator DefaultOperator { get; set; }
        public string DefaultField { get; set; }
        public Func<string, IElasticType> GetFieldMappingFunc { get; set; }
    }

    public static class ElasticQueryVisitorContextExtensions {
        public static IElasticType GetFieldMapping(this IElasticQueryVisitorContext context, string field) {
            return context.GetFieldMappingFunc?.Invoke(field);
        }

        public static bool IsFieldAnalyzed(this IElasticQueryVisitorContext context, string field) {
            if (String.IsNullOrEmpty(field))
                return true;

            var mapping = context.GetFieldMapping(field) as StringMapping;
            if (mapping == null)
                return false;

            return mapping.Index == FieldIndexOption.Analyzed || mapping.Index == null;
        }

        public static bool IsNestedFieldType(this IElasticQueryVisitorContext context, string field) {
            if (String.IsNullOrEmpty(field))
                return false;

            var mapping = context.GetFieldMapping(field) as NestedObjectMapping;
            return mapping != null;
        }

        public static bool IsGeoFieldType(this IElasticQueryVisitorContext context, string field) {
            if (String.IsNullOrEmpty(field))
                return false;

            var mapping = context.GetFieldMapping(field) as GeoPointMapping;
            return mapping != null;
        }
    }
}