using System;
using Foundatio.Parsers.LuceneQueries.Visitors;
using Nest;
using System.Linq;
using System.Threading.Tasks;

namespace Foundatio.Parsers.ElasticQueries.Visitors {
    public class ElasticQueryVisitorContext : QueryVisitorContext, IQueryVisitorContextWithIncludeResolver, IQueryVisitorContextWithAliasResolver, IElasticQueryVisitorContext {
        public Operator DefaultOperator { get; set; } = Operator.And;
        public bool UseScoring { get; set; }
        public string DefaultField { get; set; }
        public Func<string, IProperty> GetPropertyMappingFunc { get; set; }
        public Func<string, Task<string>> IncludeResolver { get; set; }
        public AliasResolver RootAliasResolver { get; set; }
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
            var textProperty = property as ITextProperty;
            if (textProperty != null)
                return !textProperty.Index.HasValue || textProperty.Index.Value;

#pragma warning disable 618
            var stringMapping = property as IStringProperty;
            if (stringMapping != null)
                return stringMapping.Index == FieldIndexOption.Analyzed || stringMapping.Index == null;
#pragma warning restore 618

            return false;
        }

        public static bool IsNestedPropertyType(this IElasticQueryVisitorContext context, string field) {
            if (String.IsNullOrEmpty(field))
                return false;

            var mapping = context.GetPropertyMapping(field) as INestedProperty;
            return mapping != null;
        }

        public static bool IsGeoPropertyType(this IElasticQueryVisitorContext context, string field) {
            if (String.IsNullOrEmpty(field))
                return false;

            var mapping = context.GetPropertyMapping(field) as IGeoPointProperty;
            return mapping != null;
        }

        public static bool IsNumericPropertyType(this IElasticQueryVisitorContext context, string field) {
            if (String.IsNullOrEmpty(field))
                return false;

            var mapping = context.GetPropertyMapping(field) as INumberProperty;
            return mapping != null;
        }

        public static bool IsBooleanPropertyType(this IElasticQueryVisitorContext context, string field) {
            if (String.IsNullOrEmpty(field))
                return false;

            var mapping = context.GetPropertyMapping(field) as IBooleanProperty;
            return mapping != null;
        }
    }
}
