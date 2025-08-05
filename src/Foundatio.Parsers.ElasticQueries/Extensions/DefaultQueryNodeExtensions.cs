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

        QueryBase query;
        string field = node.UnescapedField;
        string[] defaultFields = node.GetDefaultFields(elasticContext.DefaultFields);
        if (field == null && defaultFields != null && defaultFields.Length == 1)
            field = defaultFields[0];

        if (elasticContext.MappingResolver.IsPropertyAnalyzed(field))
        {
            string[] fields = !String.IsNullOrEmpty(field) ? [field] : defaultFields;

            // Handle wildcard query string case
            if (!node.IsQuotedTerm && node.UnescapedTerm.EndsWith("*"))
            {
                query = new QueryStringQuery
                {
                    Fields = fields,
                    AllowLeadingWildcard = false,
                    AnalyzeWildcard = true,
                    Query = node.UnescapedTerm
                };
                return query;
            }

            // If a single field, use single Match or MatchPhrase query
            if (fields != null && fields.Length == 1)
            {
                if (node.IsQuotedTerm)
                {
                    query = new MatchPhraseQuery
                    {
                        Field = fields[0],
                        Query = node.UnescapedTerm
                    };
                }
                else
                {
                    query = new MatchQuery
                    {
                        Field = fields[0],
                        Query = node.UnescapedTerm
                    };
                }
                return query;
            }

            // Multiple fields: split into nested groups and non-nested
            var shouldQueries = new List<QueryBase>();

            // Group nested fields by prefix (before dot)
            var nestedGroups = fields
                .Where(f => !String.IsNullOrEmpty(f) && f.Contains('.'))
                .GroupBy(f => f.Substring(0, f.IndexOf('.')));

            // Non-nested fields (no dot)
            string[] nonNestedFields = [.. fields.Where(f => String.IsNullOrEmpty(f) || !f.Contains('.'))];

            // Add non-nested query if any
            if (nonNestedFields.Length == 1)
            {
                if (node.IsQuotedTerm)
                {
                    shouldQueries.Add(new MatchPhraseQuery
                    {
                        Field = nonNestedFields[0],
                        Query = node.UnescapedTerm
                    });
                }
                else
                {
                    shouldQueries.Add(new MatchQuery
                    {
                        Field = nonNestedFields[0],
                        Query = node.UnescapedTerm
                    });
                }
            }
            else if (nonNestedFields.Length > 1)
            {
                var mmq = new MultiMatchQuery
                {
                    Fields = nonNestedFields,
                    Query = node.UnescapedTerm
                };
                if (node.IsQuotedTerm)
                    mmq.Type = TextQueryType.Phrase;

                shouldQueries.Add(mmq);
            }

            // Add nested queries per nested group
            foreach (var group in nestedGroups)
            {
                string nestedPath = group.Key;

                var nestedShouldQueries = group.Select(f => (QueryBase)(node.IsQuotedTerm
                    ? new MatchPhraseQuery { Field = f, Query = node.UnescapedTerm }
                    : new MatchQuery { Field = f, Query = node.UnescapedTerm })).ToList();

                QueryBase nestedInnerQuery;
                if (nestedShouldQueries.Count == 1)
                    nestedInnerQuery = nestedShouldQueries[0];
                else
                    nestedInnerQuery = new BoolQuery { Should = nestedShouldQueries.Select(q => (QueryContainer)q), MinimumShouldMatch = 1 };

                var nestedQuery = new NestedQuery
                {
                    Path = nestedPath,
                    Query = nestedInnerQuery
                };

                shouldQueries.Add(nestedQuery);
            }

            query = new BoolQuery
            {
                Should = shouldQueries.Select(q => (QueryContainer)q),
                MinimumShouldMatch = 1
            };
        }
        else
        {
            // Non-analyzed field path: handle prefix and term queries
            if (!node.IsQuotedTerm && node.UnescapedTerm.EndsWith("*"))
            {
                query = new PrefixQuery
                {
                    Field = field,
                    Value = node.UnescapedTerm.TrimEnd('*')
                };
            }
            else
            {
                query = new TermQuery
                {
                    Field = field,
                    Value = node.UnescapedTerm
                };
            }
        }

        return query;
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
