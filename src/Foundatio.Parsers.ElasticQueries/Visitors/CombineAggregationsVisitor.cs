using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using Foundatio.Parsers.ElasticQueries.Extensions;
using Foundatio.Parsers.LuceneQueries.Extensions;
using Foundatio.Parsers.LuceneQueries.Nodes;
using Foundatio.Parsers.LuceneQueries.Visitors;
using Nest;

namespace Foundatio.Parsers.ElasticQueries.Visitors;

public class CombineAggregationsVisitor : ChainableQueryVisitor
{
    private const string NestedPrefix = "nested.";

    public override async Task VisitAsync(GroupNode node, IQueryVisitorContext context)
    {
        await base.VisitAsync(node, context).ConfigureAwait(false);

        if (context is not IElasticQueryVisitorContext elasticContext)
            throw new ArgumentException("Context must be of type IElasticQueryVisitorContext", nameof(context));

        var container = await GetParentContainerAsync(node, context);
        var termsAggregation = container as ITermsAggregation;

        var parentBucket = container as BucketAggregationBase;
        var childAggregations = new Dictionary<string, AggregationContainer>();

        foreach (var child in node.Children.OfType<IFieldQueryNode>())
        {
            var aggregation = await child.GetAggregationAsync(() => child.GetDefaultAggregationAsync(context));
            if (aggregation == null)
            {
                var termNode = child as TermNode;
                if (termNode != null && termsAggregation != null)
                {
                    // Accumulate @exclude values as a list
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
                        if (!String.IsNullOrEmpty(termNode.Term) && Int32.TryParse(termNode.UnescapedTerm, out int parsedMinCount))
                            termsAggregation.MinimumDocumentCount = parsedMinCount;
                    }
                    else if (termNode.Field == "@max")
                    {
                        if (!String.IsNullOrEmpty(termNode.Term) && Int32.TryParse(termNode.UnescapedTerm, out int parsedMaxCount))
                            termsAggregation.Size = parsedMaxCount;
                    }
                }

                if (termNode != null && container is ITopHitsAggregation topHitsAggregation)
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
                    topHitsAggregation.Source = filter;
                }

                if (termNode != null && container is IDateHistogramAggregation dateHistogramAggregation)
                {
                    if (termNode.Field == "@missing")
                    {
                        DateTime? missingValue = null;
                        if (!string.IsNullOrEmpty(termNode.Term) && DateTime.TryParse(termNode.Term, out var parsedMissingDate))
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

            childAggregations[((IAggregation)aggregation).Name] = (AggregationContainer)aggregation;

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

        if (parentBucket != null)
        {
            bool containsNestedField = node.Children
                .OfType<IFieldQueryNode>()
                .Any(c => !String.IsNullOrEmpty(c.Field) && c.Field.StartsWith(NestedPrefix, StringComparison.OrdinalIgnoreCase));

            if (containsNestedField)
            {
                parentBucket.Aggregations ??= [];

                bool nestedExists = parentBucket.Aggregations.Any(kvp => kvp.Key == "nested_nested");

                if (!nestedExists)
                {
                    var nestedAggregation = new NestedAggregation("nested_nested")
                    {
                        Path = "nested",
                        Aggregations = []
                    };

                    var nestedAggregationContainer = new AggregationContainer
                    {
                        Nested = nestedAggregation
                    };

                    parentBucket.Aggregations["nested_nested"] = nestedAggregationContainer;
                }

                var nestedAggregationContainerFromDict = parentBucket.Aggregations["nested_nested"];
                var nestedAgg = nestedAggregationContainerFromDict.Nested;

                nestedAgg.Aggregations ??= [];

                foreach (var kvp in childAggregations)
                {
                    nestedAgg.Aggregations[kvp.Key] = kvp.Value;
                }
            }
            else
            {
                if (parentBucket.Aggregations == null)
                    parentBucket.Aggregations = [];

                foreach (var kvp in childAggregations)
                {
                    parentBucket.Aggregations[kvp.Key] = kvp.Value;
                }
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
