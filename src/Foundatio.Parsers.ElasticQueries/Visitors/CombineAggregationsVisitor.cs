using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using Foundatio.Parsers.ElasticQueries.Extensions;
using Foundatio.Parsers.LuceneQueries.Nodes;
using Foundatio.Parsers.LuceneQueries.Visitors;
using Nest;

namespace Foundatio.Parsers.ElasticQueries.Visitors {
    public class CombineAggregationsVisitor : ChainableQueryVisitor {
        public override async Task VisitAsync(GroupNode node, IQueryVisitorContext context) {
            await base.VisitAsync(node, context).ConfigureAwait(false);

            var elasticContext = context as IElasticQueryVisitorContext;
            if (elasticContext == null)
                throw new ArgumentException("Context must be of type IElasticQueryVisitorContext", nameof(context));

            var namedAgg = GetParentContainer(node, context);
            foreach (var child in node.Children.OfType<IFieldQueryNode>()) {
                var aggregation = child.GetAggregation(() => child.GetDefaultAggregation(context));
                if (aggregation != null)
                    namedAgg.Container.Aggregations.Add(aggregation.Name, aggregation.Container);
            }

            if (node.Parent == null)
                node.SetAggregation(namedAgg);
        }

        private NamedAggregationContainer GetParentContainer(IQueryNode node, IQueryVisitorContext context) {
            NamedAggregationContainer container = null;
            var currentNode = node;
            while (container == null && currentNode != null) {
                IQueryNode n = currentNode;
                container = n.GetAggregation(() => {
                    var result = n.GetDefaultAggregation(context);
                    if (result != null)
                        n.SetAggregation(result);

                    return result;
                });

                if (currentNode.Parent != null)
                    currentNode = currentNode.Parent;
                else
                    break;
            }

            if (container == null) {
                container = new NamedAggregationContainer();
                currentNode.SetAggregation(container);
            }

            if (container.Container.Aggregations == null)
                container.Container.Aggregations = new Dictionary<string, IAggregationContainer>();

            return container;
        }
    }
}
