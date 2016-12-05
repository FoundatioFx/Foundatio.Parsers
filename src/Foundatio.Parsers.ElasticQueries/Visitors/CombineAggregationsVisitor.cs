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
            
            foreach (var child in node.Children.OfType<IFieldQueryNode>()) {
                var aggregation = child.GetAggregation(() => child.GetDefaultAggregation(context));
                if (aggregation == null)
                    continue;

                container.Aggregations[((IAggregation)aggregation).Name] = (AggregationContainer)aggregation;
                if (termsAggregation != null && (child.Prefix == "-" || child.Prefix == "+")) {
                    if (termsAggregation.Order == null)
                        termsAggregation.Order = new List<TermsOrder>();

                    termsAggregation.Order.Add(new TermsOrder { Key = ((IAggregation)aggregation).Name, Order = child.Prefix == "-" ? SortOrder.Descending : SortOrder.Ascending });
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

            if (container.Aggregations == null)
                container.Aggregations = new AggregationDictionary();

            return container;
        }
    }
}
