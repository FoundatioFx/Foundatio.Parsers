using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Elastic.Clients.Elasticsearch.QueryDsl;
using Foundatio.Parsers.ElasticQueries.Visitors;
using Foundatio.Parsers.LuceneQueries.Extensions;
using Foundatio.Parsers.LuceneQueries.Nodes;
using Foundatio.Parsers.LuceneQueries.Visitors;

namespace Foundatio.Parsers.ElasticQueries.Extensions;

public static class DefaultQueryNodeExtensions
{
    public static async Task<Query> GetDefaultQueryAsync(this IQueryNode node, IQueryVisitorContext context)
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

    public static Query GetDefaultQuery(this TermNode node, IQueryVisitorContext context)
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

    private static Query GetSingleFieldQuery(TermNode node, string field, IElasticQueryVisitorContext context)
    {
        if (context.MappingResolver.IsPropertyAnalyzed(field))
        {
            // MatchQuery treats '*' as literal; PrefixQuery only works on non-analyzed fields.
            if (!node.IsQuotedTerm && node.UnescapedTerm.EndsWith("*"))
            {
                return new QueryStringQuery(node.UnescapedTerm)
                {
                    Fields = new[] { field },
                    AllowLeadingWildcard = false,
                    AnalyzeWildcard = true
                };
            }

            if (node.IsQuotedTerm)
            {
                return new MatchPhraseQuery(field, node.UnescapedTerm);
            }

            return new MatchQuery(field, node.UnescapedTerm);
        }

        if (!node.IsQuotedTerm && node.UnescapedTerm.EndsWith("*"))
        {
            return new PrefixQuery(field, node.UnescapedTerm.TrimEnd('*'));
        }

        return new TermQuery(field, node.UnescapedTerm);
    }

    private static Query GetMultiFieldQuery(TermNode node, string[] fields, IElasticQueryVisitorContext context)
    {
        if (!node.IsQuotedTerm && node.UnescapedTerm.EndsWith("*"))
        {
            var wildcardQuery = new QueryStringQuery(node.UnescapedTerm)
            {
                AllowLeadingWildcard = false,
                AnalyzeWildcard = true
            };
            if (fields is { Length: > 0 })
                wildcardQuery.Fields = fields;
            return wildcardQuery;
        }

        var query = new MultiMatchQuery(node.UnescapedTerm);
        if (fields is { Length: > 0 })
            query.Fields = fields;
        if (node.IsQuotedTerm)
            query.Type = TextQueryType.Phrase;

        return query;
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

    private static Query GetSplitNestedQuery(TermNode node, Dictionary<string, List<string>> fieldsByNestedPath, IElasticQueryVisitorContext context)
    {
        var queryList = new List<Query>();

        foreach (var (nestedPath, fields) in fieldsByNestedPath)
        {
            Query query = fields.Count == 1
                ? GetSingleFieldQuery(node, fields[0], context)
                : GetMultiFieldQuery(node, fields.ToArray(), context);

            if (!String.IsNullOrEmpty(nestedPath))
            {
                queryList.Add(new NestedQuery(nestedPath, query));
            }
            // Flatten inner should clauses to avoid unnecessary bool nesting.
            else if (query is { Bool: { Should: not null } boolQuery })
            {
                foreach (var shouldClause in boolQuery.Should)
                    queryList.Add(shouldClause);
            }
            else
            {
                queryList.Add(query);
            }
        }

        return new BoolQuery { Should = queryList };
    }

    public static async Task<Query> GetDefaultQueryAsync(this TermRangeNode node, IQueryVisitorContext context)
    {
        if (context is not IElasticQueryVisitorContext elasticContext)
            throw new ArgumentException("Context must be of type IElasticQueryVisitorContext", nameof(context));

        string field = node.UnescapedField;
        if (elasticContext.MappingResolver.IsDatePropertyType(field))
        {
            var range = new DateRangeQuery(field) { TimeZone = node.Boost ?? node.GetTimeZone(await elasticContext.GetTimeZoneAsync()) };
            if (!String.IsNullOrWhiteSpace(node.UnescapedMin) && node.UnescapedMin != "*")
            {
                if (node.MinInclusive.HasValue && !node.MinInclusive.Value)
                    range.Gt = node.UnescapedMin;
                else
                    range.Gte = node.UnescapedMin;
            }

            if (!String.IsNullOrWhiteSpace(node.UnescapedMax) && node.UnescapedMax != "*")
            {
                if (node.MaxInclusive.HasValue && !node.MaxInclusive.Value)
                    range.Lt = node.UnescapedMax;
                else
                    range.Lte = node.UnescapedMax;
            }

            return range;
        }
        else
        {
            var range = new TermRangeQuery(field);
            if (!String.IsNullOrWhiteSpace(node.UnescapedMin) && node.UnescapedMin != "*")
            {
                if (node.MinInclusive.HasValue && !node.MinInclusive.Value)
                    range.Gt = node.UnescapedMin;
                else
                    range.Gte = node.UnescapedMin;
            }

            if (!String.IsNullOrWhiteSpace(node.UnescapedMax) && node.UnescapedMax != "*")
            {
                if (node.MaxInclusive.HasValue && !node.MaxInclusive.Value)
                    range.Lt = node.UnescapedMax;
                else
                    range.Lte = node.UnescapedMax;
            }

            return range;
        }
    }

    public static Query GetDefaultQuery(this ExistsNode node, IQueryVisitorContext context)
    {
        return new ExistsQuery(node.UnescapedField);
    }

    public static Query GetDefaultQuery(this MissingNode node, IQueryVisitorContext context)
    {
        return new BoolQuery
        {
            MustNot =
            [
                new ExistsQuery(node.UnescapedField)
            ]
        };
    }
}
