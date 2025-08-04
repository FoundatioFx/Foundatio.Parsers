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
            // Map aggregation names to their originating field nodes to avoid invalid casts
            var aggNameToFieldNode = new Dictionary<string, IFieldQueryNode>();
            foreach (var child in node.Children.OfType<IFieldQueryNode>())
            {
                var aggregation = await child.GetAggregationAsync(() => child.GetDefaultAggregationAsync(context));
                if (aggregation != null)
                {
                    string name = ((IAggregation)aggregation).Name;
                    aggNameToFieldNode[name] = child;
                    childAggregations[name] = (AggregationContainer)aggregation;
                }
            }

            // Get distinct nested paths from child fields
            var nestedPaths = aggNameToFieldNode.Values
                .Select(c => GetNestedPath(c.Field))
                .Where(np => !String.IsNullOrEmpty(np))
                .Distinct()
                .ToList();

            if (parentBucket.Aggregations == null)
                parentBucket.Aggregations = new AggregationDictionary();

            foreach (string nestedPath in nestedPaths)
            {
                // Create nested aggregation name based on the path
                string nestedAggName = $"nested_{nestedPath}";
                
                // Try to find existing nested aggregation container by name
                bool nestedExists = parentBucket.Aggregations.Any(kvp => kvp.Key == nestedAggName);

                if (!nestedExists)
                {
                    // Create new nested aggregation
                    var nestedAggregation = new NestedAggregation(nestedAggName)
                    {
                        Path = nestedPath,
                        Aggregations = []
                    };

                    var nestedAggContainer = new AggregationContainer
                    {
                        Nested = nestedAggregation
                    };

                    parentBucket.Aggregations[nestedAggName] = nestedAggContainer;
                }

                var nestedAgg = parentBucket.Aggregations[nestedAggName].Nested;
                nestedAgg.Aggregations ??= [];

                // Add child aggregations belonging to this nested path
                foreach (var kvp in childAggregations)
                {
                    if (aggNameToFieldNode.TryGetValue(kvp.Key, out var fieldNode) &&
                        !String.IsNullOrEmpty(fieldNode.Field) &&
                        fieldNode.Field.StartsWith($"{nestedPath}.", StringComparison.OrdinalIgnoreCase))
                    {
                        nestedAgg.Aggregations[kvp.Key] = kvp.Value;
                    }
                }
            }

            // Add non-nested child aggregations directly under parentBucket
            foreach (var kvp in childAggregations)
            {
                if (aggNameToFieldNode.TryGetValue(kvp.Key, out var fieldNode))
                {
                    bool isNested = false;
                    if (!String.IsNullOrEmpty(fieldNode.Field))
                    {
                        string path = GetNestedPath(fieldNode.Field);
                        if (!String.IsNullOrEmpty(path) && nestedPaths.Contains(path))
                            isNested = true;
                    }
                    if (!isNested)
                    {
                        parentBucket.Aggregations[kvp.Key] = kvp.Value;
                    }
                }
                else
                {
                    // If no field info, assume not nested and add
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

    private static string GetNestedPath(string field)
    {
        if (String.IsNullOrEmpty(field))
            return null;

        int dotIndex = field.IndexOf('.');
        return dotIndex > 0 ? field.Substring(0, dotIndex) : null;
    }
}
