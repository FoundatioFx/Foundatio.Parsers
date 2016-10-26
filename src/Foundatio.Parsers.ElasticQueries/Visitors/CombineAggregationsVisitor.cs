using System;
using System.Linq;
using Foundatio.Parsers.ElasticQueries.Extensions;
using Foundatio.Parsers.LuceneQueries.Nodes;
using Foundatio.Parsers.LuceneQueries.Visitors;
using Nest;

namespace Foundatio.Parsers.ElasticQueries.Visitors {
    public class CombineAggregationsVisitor : ChainableQueryVisitor {
        public override void Visit(GroupNode node, IQueryVisitorContext context) {
            base.Visit(node, context);

            var elasticContext = context as IElasticQueryVisitorContext;
            if (elasticContext == null)
                throw new ArgumentException("Context must be of type IElasticQueryVisitorContext", nameof(context));

            var container = GetParentContainer(node, context);
            foreach (var child in node.Children.OfType<IFieldQueryNode>()) {
                var aggregation = child.GetAggregation(() => child.GetDefaultAggregation(context));
                if (aggregation == null)
                    continue;

                if (container.Aggregations == null)
                    container.Aggregations = new AggregationDictionary();

                container.Aggregations[((IAggregation)aggregation).Name] = (AggregationContainer)aggregation;
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
