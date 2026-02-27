using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Foundatio.Parsers.ElasticQueries.Visitors;
using Foundatio.Parsers.LuceneQueries.Extensions;
using Foundatio.Parsers.LuceneQueries.Nodes;
using Foundatio.Parsers.LuceneQueries.Visitors;
using Nest;

namespace Foundatio.Parsers.ElasticQueries.Extensions;

public static class DefaultQueryNodeExtensions
{
    public static async Task<QueryBase> GetDefaultQueryAsync(this IQueryNode node, IQueryVisitorContext context)
    {
        if (node is TermNode termNode)
            return termNode.GetDefaultQuery(context);

        if (node is TermRangeNode termRangeNode)
            return await termRangeNode.GetDefaultQueryAsync(context);

        if (node is ExistsNode existsNode)
            return existsNode.GetDefaultQuery(context);

        if (node is MissingNode missingNode)
            return missingNode.GetDefaultQuery(context);

        return null;
    }

    public static QueryBase GetDefaultQuery(this TermNode node, IQueryVisitorContext context)
    {
        if (context is not IElasticQueryVisitorContext elasticContext)
            throw new ArgumentException("Context must be of type IElasticQueryVisitorContext", nameof(context));

        string field = node.UnescapedField;
        string[] defaultFields = node.GetDefaultFields(elasticContext.DefaultFields);

        // If a specific field is set, use single-field query
        if (!String.IsNullOrEmpty(field))
            return GetSingleFieldQuery(node, field, elasticContext);

        // If only one default field, use single-field query
        if (defaultFields is { Length: 1 })
            return GetSingleFieldQuery(node, defaultFields[0], elasticContext);

        if (defaultFields is { Length: > 1 })
        {
            // Group fields by nested path (empty string for non-nested)
            var fieldsByNestedPath = GroupFieldsByNestedPath(defaultFields, elasticContext);

            // If all fields are non-nested (single group with empty key), use multi_match
            if (fieldsByNestedPath.Count == 1 && fieldsByNestedPath.ContainsKey(String.Empty))
            {
                return GetMultiFieldQuery(node, defaultFields, elasticContext);
            }

            // Otherwise, split into separate queries for each group
            return GetSplitNestedQuery(node, fieldsByNestedPath, elasticContext);
        }

        // Fallback for no fields
        return GetMultiFieldQuery(node, defaultFields, elasticContext);
    }

    private static QueryBase GetSingleFieldQuery(TermNode node, string field, IElasticQueryVisitorContext context)
    {
        if (context.MappingResolver.IsPropertyAnalyzed(field))
        {
            // MatchQuery treats '*' as literal; PrefixQuery only works on non-analyzed fields.
            if (!node.IsQuotedTerm && node.UnescapedTerm.EndsWith("*"))
            {
                return new QueryStringQuery
                {
                    Fields = Infer.Fields(field),
                    AllowLeadingWildcard = false,
                    AnalyzeWildcard = true,
                    Query = node.UnescapedTerm
                };
            }

            if (node.IsQuotedTerm)
            {
                return new MatchPhraseQuery
                {
                    Field = field,
                    Query = node.UnescapedTerm
                };
            }

            return new MatchQuery
            {
                Field = field,
                Query = node.UnescapedTerm
            };
        }

        if (!node.IsQuotedTerm && node.UnescapedTerm.EndsWith("*"))
        {
            return new PrefixQuery
            {
                Field = field,
                Value = node.UnescapedTerm.TrimEnd('*')
            };
        }

        // For non-analyzed fields, try to convert value to appropriate type
        object termValue = GetTypedValue(node.UnescapedTerm, field, context);

        return new TermQuery
        {
            Field = field,
            Value = termValue
        };
    }

    private static object GetTypedValue(string value, string field, IElasticQueryVisitorContext context)
    {
        var fieldType = context.MappingResolver.GetFieldType(field);

        return fieldType switch
        {
            FieldType.Integer or FieldType.Short or FieldType.Byte when Int32.TryParse(value, out int intValue) => intValue,
            FieldType.Long when Int64.TryParse(value, out long longValue) => longValue,
            FieldType.Float or FieldType.HalfFloat when Single.TryParse(value, out float floatValue) => floatValue,
            FieldType.Double or FieldType.ScaledFloat when Double.TryParse(value, out double doubleValue) => doubleValue,
            FieldType.Boolean when Boolean.TryParse(value, out bool boolValue) => boolValue,
            _ => value // Return as string for other types (keyword, date, ip, etc.)
        };
    }

    private static QueryBase GetMultiFieldQuery(TermNode node, string[] fields, IElasticQueryVisitorContext context)
    {
        // Handle null or empty fields - use default multi_match behavior
        if (fields is null or { Length: 0 })
        {
            var defaultQuery = new MultiMatchQuery
            {
                Fields = fields,
                Query = node.UnescapedTerm
            };
            if (node.IsQuotedTerm)
                defaultQuery.Type = TextQueryType.Phrase;
            return defaultQuery;
        }

        // Split fields by analyzed vs non-analyzed
        var analyzedFields = new List<string>();
        var nonAnalyzedFields = new List<string>();

        foreach (string field in fields)
        {
            if (context.MappingResolver.IsPropertyAnalyzed(field))
                analyzedFields.Add(field);
            else
                nonAnalyzedFields.Add(field);
        }

        // If all fields are of the same type, use simple query
        if (nonAnalyzedFields.Count == 0)
        {
            return GetAnalyzedFieldsQuery(node, analyzedFields.ToArray());
        }

        if (analyzedFields.Count == 0)
        {
            return GetNonAnalyzedFieldsQuery(node, nonAnalyzedFields, context);
        }

        // multi_match doesn't work well across analyzed + non-analyzed field types,
        // so split into separate queries and combine with bool should.
        var queries = new List<QueryBase>();

        // Add query for analyzed fields
        queries.Add(GetAnalyzedFieldsQuery(node, analyzedFields.ToArray()));

        // Add individual queries for non-analyzed fields
        foreach (string field in nonAnalyzedFields)
        {
            queries.Add(GetSingleFieldQuery(node, field, context));
        }

        return new BoolQuery
        {
            Should = queries.Select(q => (QueryContainer)q).ToList()
        };
    }

    private static QueryBase GetAnalyzedFieldsQuery(TermNode node, string[] fields)
    {
        if (!node.IsQuotedTerm && node.UnescapedTerm.EndsWith("*"))
        {
            return new QueryStringQuery
            {
                Fields = fields,
                AllowLeadingWildcard = false,
                AnalyzeWildcard = true,
                Query = node.UnescapedTerm
            };
        }

        if (fields.Length == 1)
        {
            if (node.IsQuotedTerm)
                return new MatchPhraseQuery { Field = fields[0], Query = node.UnescapedTerm };

            return new MatchQuery { Field = fields[0], Query = node.UnescapedTerm };
        }

        var query = new MultiMatchQuery
        {
            Fields = fields,
            Query = node.UnescapedTerm
        };
        if (node.IsQuotedTerm)
            query.Type = TextQueryType.Phrase;

        return query;
    }

    private static QueryBase GetNonAnalyzedFieldsQuery(TermNode node, List<string> fields, IElasticQueryVisitorContext context)
    {
        // For a single non-analyzed field, use single query
        if (fields.Count == 1)
            return GetSingleFieldQuery(node, fields[0], context);

        // Multiple non-analyzed fields - combine with bool should
        var queries = fields.Select(f => GetSingleFieldQuery(node, f, context)).ToList();
        return new BoolQuery
        {
            Should = queries.Select(q => (QueryContainer)q).ToList()
        };
    }

    private static Dictionary<string, List<string>> GroupFieldsByNestedPath(string[] fields, IElasticQueryVisitorContext context)
    {
        var result = new Dictionary<string, List<string>>();

        foreach (string field in fields)
        {
            // Use empty string for non-nested fields, actual path for nested
            string nestedPath = GetNestedPath(field, context) ?? String.Empty;

            if (!result.ContainsKey(nestedPath))
                result[nestedPath] = new List<string>();

            result[nestedPath].Add(field);
        }

        return result;
    }

    private static string GetNestedPath(string fullName, IElasticQueryVisitorContext context)
    {
        string[] nameParts = fullName?.Split('.');

        if (nameParts is null or { Length: 0 })
            return null;

        var builder = new StringBuilder();
        for (int i = 0; i < nameParts.Length; i++)
        {
            if (i > 0)
                builder.Append('.');

            builder.Append(nameParts[i]);

            string fieldName = builder.ToString();
            if (context.MappingResolver.IsNestedPropertyType(fieldName))
                return fieldName;
        }

        return null;
    }

    private static QueryBase GetSplitNestedQuery(TermNode node, Dictionary<string, List<string>> fieldsByNestedPath, IElasticQueryVisitorContext context)
    {
        var queryContainers = new List<QueryContainer>();

        foreach (var (nestedPath, fields) in fieldsByNestedPath)
        {
            QueryBase query = fields.Count == 1
                ? GetSingleFieldQuery(node, fields[0], context)
                : GetMultiFieldQuery(node, fields.ToArray(), context);

            if (!String.IsNullOrEmpty(nestedPath))
            {
                queryContainers.Add(new NestedQuery
                {
                    Path = nestedPath,
                    Query = query
                });
            }
            // Flatten inner should clauses to avoid unnecessary bool nesting.
            else if (query is BoolQuery boolQuery && boolQuery.Should is not null)
            {
                foreach (var shouldClause in boolQuery.Should)
                    queryContainers.Add(shouldClause);
            }
            else
            {
                queryContainers.Add(query);
            }
        }

        return new BoolQuery { Should = queryContainers };
    }

    public static async Task<QueryBase> GetDefaultQueryAsync(this TermRangeNode node, IQueryVisitorContext context)
    {
        if (context is not IElasticQueryVisitorContext elasticContext)
            throw new ArgumentException("Context must be of type IElasticQueryVisitorContext", nameof(context));

        string field = node.UnescapedField;
        if (elasticContext.MappingResolver.IsDatePropertyType(field))
        {
            var range = new DateRangeQuery { Field = field, TimeZone = node.Boost ?? node.GetTimeZone(await elasticContext.GetTimeZoneAsync()) };
            if (!String.IsNullOrWhiteSpace(node.UnescapedMin) && node.UnescapedMin != "*")
            {
                if (node.MinInclusive.HasValue && !node.MinInclusive.Value)
                    range.GreaterThan = node.UnescapedMin;
                else
                    range.GreaterThanOrEqualTo = node.UnescapedMin;
            }

            if (!String.IsNullOrWhiteSpace(node.UnescapedMax) && node.UnescapedMax != "*")
            {
                if (node.MaxInclusive.HasValue && !node.MaxInclusive.Value)
                    range.LessThan = node.UnescapedMax;
                else
                    range.LessThanOrEqualTo = node.UnescapedMax;
            }

            return range;
        }
        else
        {
            var range = new TermRangeQuery { Field = field };
            if (!String.IsNullOrWhiteSpace(node.UnescapedMin) && node.UnescapedMin != "*")
            {
                if (node.MinInclusive.HasValue && !node.MinInclusive.Value)
                    range.GreaterThan = node.UnescapedMin;
                else
                    range.GreaterThanOrEqualTo = node.UnescapedMin;
            }

            if (!String.IsNullOrWhiteSpace(node.UnescapedMax) && node.UnescapedMax != "*")
            {
                if (node.MaxInclusive.HasValue && !node.MaxInclusive.Value)
                    range.LessThan = node.UnescapedMax;
                else
                    range.LessThanOrEqualTo = node.UnescapedMax;
            }

            return range;
        }
    }

    public static QueryBase GetDefaultQuery(this ExistsNode node, IQueryVisitorContext context)
    {
        return new ExistsQuery { Field = node.UnescapedField };
    }

    public static QueryBase GetDefaultQuery(this MissingNode node, IQueryVisitorContext context)
    {
        return new BoolQuery
        {
            MustNot =
            [
                new ExistsQuery
                {
                    Field = node.UnescapedField
                }
            ]
        };
    }
}
