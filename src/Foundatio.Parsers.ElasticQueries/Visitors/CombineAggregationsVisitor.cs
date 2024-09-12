using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using Elastic.Clients.Elasticsearch;
using Elastic.Clients.Elasticsearch.Aggregations;
using Elastic.Clients.Elasticsearch.Core.Search;
using Foundatio.Parsers.ElasticQueries.Extensions;
using Foundatio.Parsers.LuceneQueries.Extensions;
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
            {
                var termNode = child as TermNode;
                if (termNode != null && termsAggregation != null)
                {
                    // TODO: Move these to the default aggs method using a visitor to walk down the tree to gather them but not going into any sub groups
                    if (termNode.Field == "@exclude")
                    {
                        termsAggregation.Exclude = termsAggregation.Exclude.AddValue(termNode.UnescapedTerm);
                    }
                    else if (termNode.Field == "@include")
                    {
                        termsAggregation.Include = termsAggregation.Include.AddValue(termNode.UnescapedTerm);
                    }
                    else if (termNode.Field == "@missing")
                    {
                        termsAggregation.Missing = termNode.UnescapedTerm;
                    }
                    else if (termNode.Field == "@min")
                    {
                        int? minCount = null;
                        if (!String.IsNullOrEmpty(termNode.Term) && Int32.TryParse(termNode.UnescapedTerm, out int parsedMinCount))
                            minCount = parsedMinCount;

                        termsAggregation.MinDocCount = minCount;
                    }
                }

                if (termNode != null && container.Value is TopHitsAggregation topHitsAggregation)
                {
                    var filter = node.GetSourceFilter(() => new SourceFilter());
                    if (termNode.Field == "@exclude")
                    {
                        if (filter.Excludes == null)
                            filter.Excludes = termNode.UnescapedTerm;
                        else
                            filter.Excludes.And(termNode.UnescapedTerm);
                    }
                    else if (termNode.Field == "@include")
                    {
                        if (filter.Includes == null)
                            filter.Includes = termNode.UnescapedTerm;
                        else
                            filter.Includes.And(termNode.UnescapedTerm);
                    }
                    topHitsAggregation.Source = new SourceConfig(filter);
                }

                if (termNode != null && container.Value is DateHistogramAggregation dateHistogramAggregation)
                {
                    if (termNode.Field == "@missing")
                    {
                        DateTime? missingValue = null;
                        if (!String.IsNullOrEmpty(termNode.Term) && DateTime.TryParse(termNode.Term, out var parsedMissingDate))
                            missingValue = parsedMissingDate;

                        dateHistogramAggregation.Missing = missingValue;
                    }
                    else if (termNode.Field == "@offset")
                    {
                        dateHistogramAggregation.Offset = termNode.IsExcluded() ? "-" + termNode.Term : termNode.Term;
                    }
                }

                continue;
            }

            if (container.Value is BucketAggregationBase bucketContainer)
            {
                if (bucketContainer.Aggregations == null)
                    bucketContainer.Aggregations = new AggregationDictionary();

                bucketContainer.Aggregations[aggregation.Name] = aggregation.Value;
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
