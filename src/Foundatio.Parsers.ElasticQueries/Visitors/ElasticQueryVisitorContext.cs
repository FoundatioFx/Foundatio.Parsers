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
        public IncludeResolver IncludeResolver { get; set; }
        public QueryFieldResolver FieldResolver { get; set; }
        public Func<QueryValidationInfo, Task<bool>> Validator { get; set; }
        public QueryValidationInfo ValidationInfo { get; set; }
    }

    public static class ElasticQueryVisitorContextExtensions {
        public static (string ResolvedField, IProperty Mapping) GetPropertyMapping(this IElasticQueryVisitorContext context, string field, bool resolveAlias = true) {
            if (String.IsNullOrEmpty(field))
                return (field, null);

            string resolvedField = field;
            var property = context.GetPropertyMappingFunc?.Invoke(field);
            
            if (resolveAlias && property is IFieldAliasProperty fieldAlias) {
                resolvedField = fieldAlias.Path.Name;
                property = context.GetPropertyMappingFunc?.Invoke(resolvedField);
            }

            return (resolvedField, property);
        }

        public static string GetNonAnalyzedFieldName(this IElasticQueryVisitorContext context, string field, string preferredSubField = null) {
            if (String.IsNullOrEmpty(field))
                return field;

            var property = context.GetPropertyMapping(field);

            if (property.Mapping == null || !context.IsPropertyAnalyzed(property.Mapping))
                return field;

            var multiFieldProperty = property.Mapping as ICoreProperty;
            if (multiFieldProperty?.Fields == null)
                return property.ResolvedField;
            
            var nonAnalyzedProperty = multiFieldProperty.Fields.OrderByDescending(kvp => kvp.Key.Name == preferredSubField).FirstOrDefault(kvp => {
                if (kvp.Value is IKeywordProperty)
                    return true;

                if (!context.IsPropertyAnalyzed(kvp.Value))
                    return true;

                return false;
            });

            if (nonAnalyzedProperty.Value != null)
                return property.ResolvedField + "." + nonAnalyzedProperty.Key.Name;

            return property.ResolvedField;
        }

        public static bool IsPropertyAnalyzed(this IElasticQueryVisitorContext context, string field) {
            // assume default is analyzed
            if (String.IsNullOrEmpty(field))
                return true;

            var property = context.GetPropertyMapping(field);
            if (property.Mapping == null)
                return false;

            return context.IsPropertyAnalyzed(property.Mapping);
        }

        public static bool IsPropertyAnalyzed(this IElasticQueryVisitorContext context, IProperty property) {
            if (property is ITextProperty textProperty)
                return !textProperty.Index.HasValue || textProperty.Index.Value;

            return false;
        }

        public static bool IsNestedPropertyType(this IElasticQueryVisitorContext context, string field) {
            if (String.IsNullOrEmpty(field))
                return false;

            return context.GetPropertyMapping(field).Mapping is INestedProperty;
        }

        public static bool IsGeoPropertyType(this IElasticQueryVisitorContext context, string field) {
            if (String.IsNullOrEmpty(field))
                return false;

            return context.GetPropertyMapping(field).Mapping is IGeoPointProperty;
        }

        public static bool IsNumericPropertyType(this IElasticQueryVisitorContext context, string field) {
            if (String.IsNullOrEmpty(field))
                return false;

            return context.GetPropertyMapping(field).Mapping is INumberProperty;
        }

        public static bool IsBooleanPropertyType(this IElasticQueryVisitorContext context, string field) {
            if (String.IsNullOrEmpty(field))
                return false;

            return context.GetPropertyMapping(field).Mapping is IBooleanProperty;
        }

        public static bool IsDatePropertyType(this IElasticQueryVisitorContext context, string field) {
            if (String.IsNullOrEmpty(field))
                return false;

            return context.GetPropertyMapping(field).Mapping is IDateProperty;
        }

        public static FieldType GetFieldType(this IElasticQueryVisitorContext context, string field) {
            if (String.IsNullOrWhiteSpace(field))
                return FieldType.None;

            var property = context.GetPropertyMapping(field);

            if (property.Mapping?.Type == null)
                return FieldType.None;
            
            switch (property.Mapping.Type) {
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
