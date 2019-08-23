using System;
using Foundatio.Parsers.LuceneQueries.Visitors;
using Nest;
using System.Linq;
using System.Threading.Tasks;
using Elasticsearch.Net;

namespace Foundatio.Parsers.ElasticQueries.Visitors {
    public class ElasticQueryVisitorContext : QueryVisitorContext, IQueryVisitorContextWithIncludeResolver, IQueryVisitorContextWithFieldResolver, IElasticQueryVisitorContext, IQueryVisitorContextWithValidator {
        public Operator DefaultOperator { get; set; } = Operator.And;
        public bool UseScoring { get; set; }
        public Func<string, IProperty> GetPropertyMappingFunc { get; set; }
        public Func<string, Task<string>> IncludeResolver { get; set; }
        public QueryFieldResolver FieldResolver { get; set; }
        public Func<QueryValidationInfo, Task<bool>> Validator { get; set; }
        public QueryValidationInfo ValidationInfo { get; set; }
    }

    public static class ElasticQueryVisitorContextExtensions {
        public static IProperty GetPropertyMapping(this IElasticQueryVisitorContext context, string field) {
            if (String.IsNullOrEmpty(field))
                return null;

            return context.GetPropertyMappingFunc?.Invoke(field);
        }

        public static string GetNonAnalyzedFieldName(this IElasticQueryVisitorContext context, string field, string preferredSubField = null) {
            if (String.IsNullOrEmpty(field))
                return field;

            var property = context.GetPropertyMapping(field);

            if (property is FieldAliasProperty fieldAlias) {
                field = fieldAlias.Path.Name;
                property = context.GetPropertyMapping(field);
            }

            if (property == null || !context.IsPropertyAnalyzed(property))
                return field;

            var multiFieldProperty = property as ICoreProperty;
            if (multiFieldProperty?.Fields == null)
                return field;
            
            var nonAnalyzedProperty = multiFieldProperty.Fields.OrderByDescending(kvp => kvp.Key.Name == preferredSubField).FirstOrDefault(kvp => {
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
            // assume default is analyzed
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

        public static FieldType GetFieldType(this IElasticQueryVisitorContext context, string field) {
            if (String.IsNullOrWhiteSpace(field))
                return FieldType.None;

            var mapping = context.GetPropertyMapping(field);

            if (mapping?.Type == null)
                return FieldType.None;
            
            switch (mapping.Type) {
                case "geo_point":
                    return FieldType.GeoPoint;
                case "geo_shape":
                    return FieldType.GeoShape;
                case "ip":
                    return FieldType.Ip;
                case "binary":
                    return FieldType.Binary;
                case "keyword":
                    return FieldType.Keyword;
                case "string":
                case "text":
                    return FieldType.Text;
                case "date":
                    return FieldType.Date;
                case "boolean":
                    return FieldType.Boolean;
                case "completion":
                    return FieldType.Completion;
                case "nested":
                    return FieldType.Nested;
                case "object":
                    return FieldType.Object;
                case "murmur3":
                    return FieldType.Murmur3Hash;
                case "token_count":
                    return FieldType.TokenCount;
                case "percolator":
                    return FieldType.Percolator;
                case "integer":
                    return FieldType.Integer;
                case "long":
                    return FieldType.Long;
                case "short":
                    return FieldType.Short;
                case "byte":
                    return FieldType.Byte;
                case "float":
                    return FieldType.Float;
                case "half_float":
                    return FieldType.HalfFloat;
                case "scaled_float":
                    return FieldType.ScaledFloat;
                case "double":
                    return FieldType.Double;
                case "integer_range":
                    return FieldType.IntegerRange;
                case "float_range":
                    return FieldType.FloatRange;
                case "long_range":
                    return FieldType.LongRange;
                case "double_range":
                    return FieldType.DoubleRange;
                case "date_range":
                    return FieldType.DateRange;
                case "ip_range":
                    return FieldType.IpRange;
                default:
                    return FieldType.None;
            }
        }
    }
}
