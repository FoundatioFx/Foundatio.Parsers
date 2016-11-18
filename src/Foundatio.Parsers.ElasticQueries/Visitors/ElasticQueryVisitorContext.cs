using System;
using Foundatio.Parsers.LuceneQueries.Visitors;
using Nest;
using System.Linq;
using System.Threading.Tasks;

namespace Foundatio.Parsers.ElasticQueries.Visitors {
    public class ElasticQueryVisitorContext : QueryVisitorContext, IQueryVisitorContextWithIncludeResolver, IQueryVisitorContextWithAliasResolver, IElasticQueryVisitorContext {
        public Operator DefaultOperator { get; set; }
        public string DefaultField { get; set; }
        public Func<string, IElasticType> GetFieldMappingFunc { get; set; }
        public Func<string, Task<string>> IncludeResolver { get; set; }
        public AliasResolver RootAliasResolver { get; set; }
    }

    public static class ElasticQueryVisitorContextExtensions {
        public static IElasticType GetFieldMapping(this IElasticQueryVisitorContext context, string field) {
            return context.GetFieldMappingFunc?.Invoke(field);
        }

        public static string GetNonAnalyzedFieldName(this IElasticQueryVisitorContext context, string field) {
            if (String.IsNullOrEmpty(field))
                return field;

            var mapping = context.GetFieldMapping(field) as StringMapping;
            if (mapping == null)
                return field;

            if (mapping.Index == FieldIndexOption.Analyzed || mapping.Index == null) {
                var nonAnalyzedField = mapping.Fields.FirstOrDefault(kvp => {
                    var childMapping = kvp.Value as StringMapping;
                    if (childMapping.Index == FieldIndexOption.No || childMapping.Index == FieldIndexOption.NotAnalyzed)
                        return true;

                    return false;
                });

                if (nonAnalyzedField.Value != null)
                    return field + "." + nonAnalyzedField.Key.Name;
            }

            return field;
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