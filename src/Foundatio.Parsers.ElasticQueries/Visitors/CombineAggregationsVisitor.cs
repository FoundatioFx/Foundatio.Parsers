using System;
using System.Collections.Generic;
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

            node.SetAggregationContainer(new AggregationContainer {
                Aggregations = new Dictionary<string, IAggregationContainer> {
                    { "min_Field2", new AggregationContainer { Min = new MinAggregator { Field = "Field2" } } }
                }
            });
        }
    }
}
