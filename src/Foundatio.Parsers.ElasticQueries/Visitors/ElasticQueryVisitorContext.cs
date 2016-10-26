using System;
using Foundatio.Parsers.LuceneQueries.Visitors;
using Nest;

namespace Foundatio.Parsers.ElasticQueries.Visitors {
    public class ElasticQueryVisitorContext : QueryVisitorContextWithAliasResolver, IElasticQueryVisitorContext {
        public Operator DefaultOperator { get; set; } = Operator.And;
        public bool UseScoring { get; set; }
        public string DefaultField { get; set; }
        public Func<string, IProperty> GetPropertyMappingFunc { get; set; }
    }

    public static class ElasticQueryVisitorContextExtensions {
        public static IProperty GetPropertyMapping(this IElasticQueryVisitorContext context, string field) {
            return context.GetPropertyMappingFunc?.Invoke(field);
        }

        public static bool IsPropertyAnalyzed(this IElasticQueryVisitorContext context, string field) {
            if (String.IsNullOrEmpty(field))
                return true;

            var mapping = context.GetPropertyMapping(field) as TextProperty;
            if (mapping == null)
                return false;

            return !mapping.Index.HasValue || mapping.Index.Value;
        }

        public static bool IsNestedPropertyType(this IElasticQueryVisitorContext context, string field) {
            if (String.IsNullOrEmpty(field))
                return false;

            var mapping = context.GetPropertyMapping(field) as NestedProperty;
            return mapping != null;
        }

        public static bool IsGeoPropertyType(this IElasticQueryVisitorContext context, string field) {
            if (String.IsNullOrEmpty(field))
                return false;

            var mapping = context.GetPropertyMapping(field) as GeoPointProperty;
            return mapping != null;
        }

        public static bool IsNumericPropertyType(this IElasticQueryVisitorContext context, string field) {
            if (String.IsNullOrEmpty(field))
                return false;

            var mapping = context.GetPropertyMapping(field) as NumberProperty;
            return mapping != null;
        }
    }
}
