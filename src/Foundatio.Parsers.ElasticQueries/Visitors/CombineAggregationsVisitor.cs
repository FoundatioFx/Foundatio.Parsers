using System;
using System.Collections.Generic;
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
                var childContainer = child.GetAggregationContainer(() => child.GetDefaultAggregation(context));
                if (childContainer != null)
                    container.Aggregations.Add(Guid.NewGuid().ToString(), childContainer);
            }

            if (node.Parent == null)
                node.SetAggregationContainer(container);
        }

        private AggregationContainer GetParentContainer(IQueryNode node, IQueryVisitorContext context) {
            AggregationContainer container = null;
            var currentNode = node;
            while (container == null && currentNode != null) {
                IQueryNode n = currentNode;
                container = currentNode.GetAggregationContainer(() => {
                    var result = n.GetDefaultAggregation(context);
                    if (result != null)
                        n.SetAggregationContainer(result);

                    return result;
                });
                currentNode = currentNode.Parent;
            }

            if (container == null)
                container = new AggregationContainer();

            if (container.Aggregations == null)
                container.Aggregations = new Dictionary<string, IAggregationContainer>();

            return container;
        }
    }
}
