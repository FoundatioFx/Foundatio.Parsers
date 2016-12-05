using System;
using System.Linq;
using System.Threading.Tasks;
using Foundatio.Parsers.ElasticQueries.Extensions;
using Foundatio.Parsers.LuceneQueries.Nodes;
using Foundatio.Parsers.LuceneQueries.Visitors;
using Nest;
using System.Collections.Generic;

namespace Foundatio.Parsers.ElasticQueries.Visitors {
    public class CombineAggregationsVisitor : ChainableQueryVisitor {
        public override async Task VisitAsync(GroupNode node, IQueryVisitorContext context) {
            await base.VisitAsync(node, context).ConfigureAwait(false);

            var elasticContext = context as IElasticQueryVisitorContext;
            if (elasticContext == null)
                throw new ArgumentException("Context must be of type IElasticQueryVisitorContext", nameof(context));

            var container = GetParentContainer(node, context);
            var termsAggregation = container as ITermsAggregation;
            var dateHistogramAggregation = container as IDateHistogramAggregation;

            foreach (var child in node.Children.OfType<IFieldQueryNode>()) {
                var aggregation = child.GetAggregation(() => child.GetDefaultAggregation(context));
                if (aggregation == null) {
                    var termNode = child as TermNode;
                    if (termNode != null && termsAggregation != null) {
                        if (termNode.Field == "@exclude")
                            termsAggregation.Exclude = new TermsIncludeExclude { Pattern = $"{termNode.Prefix}{termNode.Term}" };
                        else if (termNode.Field == "@include")
                            termsAggregation.Include = new TermsIncludeExclude { Pattern = $"{termNode.Prefix}{termNode.Term}" };
                        else if (termNode.Field == "@missing")
                            termsAggregation.Missing = $"{termNode.Prefix}{termNode.Term}";
                        else if (termNode.Field == "@min") {
                            int? minCount = null;
                            int parsedMinCount;
                            if (!String.IsNullOrEmpty(termNode.Term) && Int32.TryParse($"{termNode.Prefix}{termNode.Term}", out parsedMinCount))
                                minCount = parsedMinCount;

                            termsAggregation.MinimumDocumentCount = minCount;
                        }
                    }

                    if (termNode != null && dateHistogramAggregation != null) {
                        if (termNode.Field == "@missing") {
                            DateTime? missingValue = null;
                            DateTime parsedMissingDate;
                            if (!String.IsNullOrEmpty(termNode.Term) && DateTime.TryParse(termNode.Term, out parsedMissingDate))
                                missingValue = parsedMissingDate;

                            dateHistogramAggregation.Missing = missingValue;
                        }
                    }

                    continue;
                }

                if (container.Aggregations == null)
                    container.Aggregations = new AggregationDictionary();

                container.Aggregations[((IAggregation)aggregation).Name] = (AggregationContainer)aggregation;
                if (child.Prefix == "-" || child.Prefix == "+") {
                    var termsAgg = aggregation as ITermsAggregation;
                    if (termsAgg != null)
                        ApplyTermsSort(termsAgg, termsAgg, child.Prefix);
                    else if (termsAggregation != null)
                        ApplyTermsSort(termsAggregation, aggregation, child.Prefix);
                }
            }

            if (node.Parent == null)
                node.SetAggregation(container);
        }

        private static void ApplyTermsSort(ITermsAggregation termsAggregation, IAggregation aggregation, string prefix) {
            if (termsAggregation.Order == null)
                termsAggregation.Order = new List<TermsOrder>();

            termsAggregation.Order.Add(new TermsOrder {
                Key = aggregation.Name,
                Order = prefix == "-" ? SortOrder.Descending : SortOrder.Ascending
            });
        }

        private BucketAggregationBase GetParentContainer(IQueryNode node, IQueryVisitorContext context) {
            BucketAggregationBase container = null;
            var currentNode = node;
            while (container == null && currentNode != null) {
                IQueryNode n = currentNode;
                container = n.GetAggregation(() => {
                    var result = n.GetDefaultAggregation(context);
                    if (result != null)
                        n.SetAggregation(result);

                    return result;
                }) as BucketAggregationBase;

                if (currentNode.Parent != null)
                    currentNode = currentNode.Parent;
                else
                    break;
            }

            if (container == null) {
                container = new ChildrenAggregation(null, null);
                currentNode.SetAggregation(container);
            }

            return container;
        }
    }
}
