using System;
using Elastic.Clients.Elasticsearch;
using Elastic.Clients.Elasticsearch.Mapping;
using Foundatio.Parsers.ElasticQueries.Visitors;
using Foundatio.Parsers.LuceneQueries.Extensions;
using Foundatio.Parsers.LuceneQueries.Nodes;
using Foundatio.Parsers.LuceneQueries.Visitors;

namespace Foundatio.Parsers.ElasticQueries.Extensions;

public static class DefaultSortNodeExtensions
{
    public static SortOptions GetDefaultSort(this TermNode node, IQueryVisitorContext context)
    {
        if (context is not IElasticQueryVisitorContext elasticContext)
            throw new ArgumentException("Context must be of type IElasticQueryVisitorContext", nameof(context));

        string field = elasticContext.MappingResolver.GetSortFieldName(node.UnescapedField);
        var fieldType = elasticContext.MappingResolver.GetFieldType(field);

        return new SortOptions
        {
            Field = new FieldSort(field)
            {
                UnmappedType = fieldType == FieldType.None ? FieldType.Keyword : fieldType,
                Order = node.IsNodeOrGroupNegated() ? SortOrder.Desc : SortOrder.Asc
            }
        };
    }
}
