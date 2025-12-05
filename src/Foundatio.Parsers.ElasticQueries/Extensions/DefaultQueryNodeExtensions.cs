using System;
using System.Collections.Generic;
using System.Linq;
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
        if (defaultFields != null && defaultFields.Length == 1)
            return GetSingleFieldQuery(node, defaultFields[0], elasticContext);

        // Multiple default fields - check if any are nested
        if (defaultFields != null && defaultFields.Length > 1)
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
        if (fields == null || fields.Length == 0)
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

        // Mixed types - combine with bool should
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
        // For a single field, use match query instead of multi_match
        if (fields.Length == 1)
        {
            string field = fields[0];
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

        // Multiple fields - use multi_match
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
        string[] nameParts = fullName?.Split('.').ToArray();

        if (nameParts == null || nameParts.Length == 0)
            return null;

        string fieldName = String.Empty;
        for (int i = 0; i < nameParts.Length; i++)
        {
            if (i > 0)
                fieldName += ".";

            fieldName += nameParts[i];

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
            QueryBase query;

            if (fields.Count == 1)
            {
                query = GetSingleFieldQuery(node, fields[0], context);
            }
            else
            {
                query = GetMultiFieldQuery(node, fields.ToArray(), context);
            }

            // Wrap in NestedQuery if this is a nested path (non-empty string)
            if (!String.IsNullOrEmpty(nestedPath))
            {
                queryContainers.Add(new NestedQuery
                {
                    Path = nestedPath,
                    Query = query
                });
            }
            else
            {
                // For non-nested fields, flatten BoolQuery should clauses if present
                if (query is BoolQuery boolQuery && boolQuery.Should != null)
                {
                    foreach (var shouldClause in boolQuery.Should)
                    {
                        queryContainers.Add(shouldClause);
                    }
                }
                else
                {
                    queryContainers.Add(query);
                }
            }
        }

        // Combine with OR (should)
        if (queryContainers.Count == 1)
        {
            // Try to unwrap single QueryContainer to QueryBase
            // Can't directly cast, so return in a minimal BoolQuery
            return new BoolQuery { Should = queryContainers };
        }

        return new BoolQuery
        {
            Should = queryContainers
        };
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
