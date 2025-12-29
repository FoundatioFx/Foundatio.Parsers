using System;
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

        Query query;
        string field = node.UnescapedField;
        string[] defaultFields = node.GetDefaultFields(elasticContext.DefaultFields);
        if (field == null && defaultFields != null && defaultFields.Length == 1)
            field = defaultFields[0];

        if (elasticContext.MappingResolver.IsPropertyAnalyzed(field))
        {
            string[] fields = !String.IsNullOrEmpty(field) ? [field] : defaultFields;

            if (!node.IsQuotedTerm && node.UnescapedTerm.EndsWith("*"))
            {
                query = new QueryStringQuery(node.UnescapedTerm)
                {
                    Fields = fields,
                    AllowLeadingWildcard = false,
                    AnalyzeWildcard = true
                };
            }
            else
            {
                if (fields != null && fields.Length == 1)
                {
                    if (node.IsQuotedTerm)
                    {
                        query = new MatchPhraseQuery(fields[0], node.UnescapedTerm);
                    }
                    else
                    {
                        query = new MatchQuery(fields[0], node.UnescapedTerm);
                    }
                }
                else
                {
                    query = new MultiMatchQuery(node.UnescapedTerm)
                    {
                        Fields = fields,
                        Type = node.IsQuotedTerm ? TextQueryType.Phrase : null
                    };
                }
            }
        }
        else
        {
            if (!node.IsQuotedTerm && node.UnescapedTerm.EndsWith("*"))
            {
                query = new PrefixQuery(field, node.UnescapedTerm.TrimEnd('*'));
            }
            else
            {
                query = new TermQuery(field, node.UnescapedTerm);
            }
        }

        return query;
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
