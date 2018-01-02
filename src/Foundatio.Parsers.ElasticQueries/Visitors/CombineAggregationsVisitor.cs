using System;
using System.Linq;
using System.Threading.Tasks;
using Foundatio.Parsers.ElasticQueries.Extensions;
using Foundatio.Parsers.LuceneQueries.Nodes;
using Foundatio.Parsers.LuceneQueries.Visitors;
using Nest;
using System.Collections.Generic;
using Foundatio.Parsers.LuceneQueries.Extensions;

namespace Foundatio.Parsers.ElasticQueries.Visitors {
    public class CombineAggregationsVisitor : ChainableQueryVisitor {
        public override async Task VisitAsync(GroupNode node, IQueryVisitorContext context) {
            await base.VisitAsync(node, context).ConfigureAwait(false);

            if (!(context is IElasticQueryVisitorContext elasticContext))
                throw new ArgumentException("Context must be of type IElasticQueryVisitorContext", nameof(context));

            var container = GetParentContainer(node, context);
            var termsAggregation = container as ITermsAggregation;
            var termsProperty = elasticContext.GetPropertyMapping(node.GetFullName());
            var dateHistogramAggregation = container as IDateHistogramAggregation;

            foreach (var child in node.Children.OfType<IFieldQueryNode>()) {
                var aggregation = child.GetAggregation(() => child.GetDefaultAggregation(context));
                if (aggregation == null) {
                    var termNode = child as TermNode;
                    if (termNode != null && termsAggregation != null) {
                        // TODO: Move these to the default aggs method using a visitor to walk down the tree to gather them but not going into any sub groups
                        if (termNode.Field == "@exclude") {
                            if (termsProperty is ITextProperty || termsProperty is IKeywordProperty)
                                termsAggregation.Exclude = new TermsExclude(termNode.UnescapedTerm);
                            else
                                termsAggregation.Exclude = new TermsExclude(new List<string> { termNode.UnescapedTerm });
                        } else if (termNode.Field == "@include") {
                            if (termsProperty is ITextProperty || termsProperty is IKeywordProperty)
                                termsAggregation.Include = new TermsInclude(termNode.UnescapedTerm);
                            else
                                termsAggregation.Include = new TermsInclude(new List<string> { termNode.UnescapedTerm });
                        } else if (termNode.Field == "@missing") {
                            termsAggregation.Missing = termNode.UnescapedTerm;
                        } else if (termNode.Field == "@min") {
                            int? minCount = null;
                            if (!String.IsNullOrEmpty(termNode.Term) && Int32.TryParse(termNode.UnescapedTerm, out int parsedMinCount))
                                minCount = parsedMinCount;

                            termsAggregation.MinimumDocumentCount = minCount;
                        }
                    }

                    if (termNode != null && dateHistogramAggregation != null) {
                        if (termNode.Field == "@missing") {
                            DateTime? missingValue = null;
                            if (!String.IsNullOrEmpty(termNode.Term) && DateTime.TryParse(termNode.Term, out var parsedMissingDate))
                                missingValue = parsedMissingDate;

                            dateHistogramAggregation.Missing = missingValue;
                        } else if (termNode.Field == "@offset") {
                            dateHistogramAggregation.Offset = termNode.IsNodeNegated() ? "-" + termNode.Term : termNode.Term;
                        }
                    }

                    continue;
                }

                if (container.Aggregations == null)
                    container.Aggregations = new AggregationDictionary();

                container.Aggregations[((IAggregation)aggregation).Name] = (AggregationContainer)aggregation;
                if (termsAggregation != null && (child.Prefix == "-" || child.Prefix == "+")) {
                    if (termsAggregation.Order == null)
                        termsAggregation.Order = new List<TermsOrder>();

                    termsAggregation.Order.Add(new TermsOrder {
                        Key = ((IAggregation)aggregation).Name,
                        Order = child.Prefix == "-" ? SortOrder.Descending : SortOrder.Ascending
                    });
                }
            }

            if (node.Parent == null)
                node.SetAggregation(container);
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
