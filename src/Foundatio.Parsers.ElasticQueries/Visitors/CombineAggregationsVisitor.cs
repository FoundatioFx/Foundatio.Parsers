using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using Elastic.Clients.Elasticsearch;
using Elastic.Clients.Elasticsearch.Aggregations;
using Elastic.Clients.Elasticsearch.Core.Search;
using Foundatio.Parsers.ElasticQueries.Extensions;
using Foundatio.Parsers.LuceneQueries.Nodes;
using Foundatio.Parsers.LuceneQueries.Visitors;

namespace Foundatio.Parsers.ElasticQueries.Visitors;

public class CombineAggregationsVisitor : ChainableQueryVisitor
{
    public override async Task VisitAsync(GroupNode node, IQueryVisitorContext context)
    {
        await base.VisitAsync(node, context).ConfigureAwait(false);

        if (context is not IElasticQueryVisitorContext elasticContext)
            throw new ArgumentException("Context must be of type IElasticQueryVisitorContext", nameof(context));

        var container = await GetParentContainerAsync(node, context);
        var termsAggregation = container.Value as TermsAggregation;

        foreach (var child in node.Children.OfType<IFieldQueryNode>())
        {
            var aggregation = await child.GetAggregationAsync(() => child.GetDefaultAggregationAsync(context));
            if (aggregation == null)
                continue;

            // Add to root container (Value is null) or to bucket aggregations
            if (container.Value == null || container.Value.IsBucketAggregation())
            {
                container.Aggregations.Add(aggregation);
            }

            if (termsAggregation != null && child.Prefix is "-" or "+")
            {
                termsAggregation.Order ??= new List<KeyValuePair<Field, SortOrder>>();
                termsAggregation.Order.Add(new KeyValuePair<Field, SortOrder>(aggregation.Name, child.Prefix == "-" ? SortOrder.Desc : SortOrder.Asc));
            }
        }

        if (node.Parent == null)
            node.SetAggregation(container);
    }

    private async Task<AggregationMap> GetParentContainerAsync(IQueryNode node, IQueryVisitorContext context)
    {
        AggregationMap container = null;
        var currentNode = node;
        while (container == null && currentNode != null)
        {
            IQueryNode n = currentNode;
            container = await n.GetAggregationAsync(async () =>
            {
                var result = await n.GetDefaultAggregationAsync(context);
                if (result != null)
                    n.SetAggregation(result);

                return result;
            });

            if (currentNode.Parent != null)
                currentNode = currentNode.Parent;
            else
                break;
        }

        if (container == null)
        {
            container = new AggregationMap(null, null);
            currentNode.SetAggregation(container);
        }

        return container;
    }
}
