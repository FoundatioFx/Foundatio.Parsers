using System;
using Foundatio.Parsers.LuceneQueries.Visitors;
using Nest;
using System.Linq;
using System.Threading.Tasks;

namespace Foundatio.Parsers.ElasticQueries.Visitors {
    public class ElasticQueryVisitorContext : QueryVisitorContext, IQueryVisitorContextWithIncludeResolver, IQueryVisitorContextWithAliasResolver, IElasticQueryVisitorContext, IQueryVisitorContextWithValidator {
        public Operator DefaultOperator { get; set; } = Operator.And;
        public bool UseScoring { get; set; }
        public string[] DefaultFields { get; set; }
        public Func<string, IProperty> GetPropertyMappingFunc { get; set; }
        public Func<string, Task<string>> IncludeResolver { get; set; }
        public AliasResolver RootAliasResolver { get; set; }
        public Func<QueryValidationInfo, Task<bool>> Validator { get; set; }
        public QueryValidationInfo ValidationInfo { get; set; }
    }

    public static class ElasticQueryVisitorContextExtensions {
        public static IProperty GetPropertyMapping(this IElasticQueryVisitorContext context, string field) {
            if (String.IsNullOrEmpty(field))
                return null;

            return context.GetPropertyMappingFunc?.Invoke(field);
        }

        public static string GetNonAnalyzedFieldName(this IElasticQueryVisitorContext context, string field) {
            if (String.IsNullOrEmpty(field))
                return field;

            var property = context.GetPropertyMapping(field);
            if (property == null || !context.IsPropertyAnalyzed(property))
                return field;

            var multiFieldProperty = property as ICoreProperty;
            if (multiFieldProperty?.Fields == null)
                return field;

            var nonAnalyzedProperty = multiFieldProperty.Fields.FirstOrDefault(kvp => {
                if (kvp.Value is IKeywordProperty)
                    return true;

                if (!context.IsPropertyAnalyzed(kvp.Value))
                    return true;

                return false;
            });

            if (nonAnalyzedProperty.Value != null)
                return field + "." + nonAnalyzedProperty.Key.Name;

            return field;
        }

        public static bool IsPropertyAnalyzed(this IElasticQueryVisitorContext context, string field) {
            if (String.IsNullOrEmpty(field))
                return true;

            var property = context.GetPropertyMapping(field);
            if (property == null)
                return false;

            return context.IsPropertyAnalyzed(property);
        }

        public static bool IsPropertyAnalyzed(this IElasticQueryVisitorContext context, IProperty property) {
            if (property is ITextProperty textProperty)
                return !textProperty.Index.HasValue || textProperty.Index.Value;

#pragma warning disable 618
            if (property is IStringProperty stringMapping)
                return stringMapping.Index == FieldIndexOption.Analyzed || stringMapping.Index == null;
#pragma warning restore 618

            return false;
        }

        public static bool IsNestedPropertyType(this IElasticQueryVisitorContext context, string field) {
            if (String.IsNullOrEmpty(field))
                return false;

            return context.GetPropertyMapping(field) is INestedProperty;
        }

        public static bool IsGeoPropertyType(this IElasticQueryVisitorContext context, string field) {
            if (String.IsNullOrEmpty(field))
                return false;

            return context.GetPropertyMapping(field) is IGeoPointProperty;
        }

        public static bool IsNumericPropertyType(this IElasticQueryVisitorContext context, string field) {
            if (String.IsNullOrEmpty(field))
                return false;

            return context.GetPropertyMapping(field) is INumberProperty;
        }

        public static bool IsBooleanPropertyType(this IElasticQueryVisitorContext context, string field) {
            if (String.IsNullOrEmpty(field))
                return false;

            return context.GetPropertyMapping(field) is IBooleanProperty;
        }

        public static bool IsDatePropertyType(this IElasticQueryVisitorContext context, string field) {
            if (String.IsNullOrEmpty(field))
                return false;

            return context.GetPropertyMapping(field) is IDateProperty;
        }
    }
}
