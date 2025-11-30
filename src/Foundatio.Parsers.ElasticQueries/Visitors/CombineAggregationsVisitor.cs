using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using Foundatio.Parsers.ElasticQueries.Extensions;
using Foundatio.Parsers.LuceneQueries.Nodes;
using Foundatio.Parsers.LuceneQueries.Visitors;
using Nest;

namespace Foundatio.Parsers.ElasticQueries.Visitors;

public class CombineAggregationsVisitor : ChainableQueryVisitor
{
    public override async Task VisitAsync(GroupNode node, IQueryVisitorContext context)
    {
        await base.VisitAsync(node, context).ConfigureAwait(false);

        if (context is not IElasticQueryVisitorContext elasticContext)
            throw new ArgumentException("Context must be of type IElasticQueryVisitorContext", nameof(context));

        var container = await GetParentContainerAsync(node, context);
        var termsAggregation = container as ITermsAggregation;

        foreach (var child in node.Children.OfType<IFieldQueryNode>())
        {
            var aggregation = await child.GetAggregationAsync(() => child.GetDefaultAggregationAsync(context));
            if (aggregation == null)
                continue;

            if (container is BucketAggregationBase bucketContainer)
            {
                if (bucketContainer.Aggregations == null)
                    bucketContainer.Aggregations = new AggregationDictionary();

                bucketContainer.Aggregations[((IAggregation)aggregation).Name] = (AggregationContainer)aggregation;
            }

            if (termsAggregation != null && (child.Prefix == "-" || child.Prefix == "+"))
            {
                if (termsAggregation.Order == null)
                    termsAggregation.Order = new List<TermsOrder>();

                termsAggregation.Order.Add(new TermsOrder
                {
                    Key = ((IAggregation)aggregation).Name,
                    Order = child.Prefix == "-" ? SortOrder.Descending : SortOrder.Ascending
                });
            }
        }

        if (node.Parent == null)
            node.SetAggregation(container);
    }

    private async Task<AggregationBase> GetParentContainerAsync(IQueryNode node, IQueryVisitorContext context)
    {
        AggregationBase container = null;
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
            container = new ChildrenAggregation(null, null);
            currentNode.SetAggregation(container);
        }

        return container;
    }
}
