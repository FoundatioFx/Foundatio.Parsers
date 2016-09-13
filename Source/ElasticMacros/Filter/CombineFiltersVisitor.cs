using System;
using System.Linq;
using ElasticQueryParser;
using Exceptionless.ElasticQueryParser.Extensions;
using Exceptionless.ElasticQueryParser.Filter.Nodes;
using Exceptionless.LuceneQueryParser.Extensions;
using Exceptionless.LuceneQueryParser.Nodes;
using Nest;

namespace Exceptionless.ElasticQueryParser.Filter {
    public class CombineFiltersVisitor : ElasticFilterNodeVisitorBase {
        private readonly ElasticQueryParserConfiguration _config;

        public CombineFiltersVisitor(ElasticQueryParserConfiguration config) {
            _config = config;
        }

        public override void Visit(FilterGroupNode node) {
            if (node.Filter != null) {
                base.Visit(node);
                return;
            }

            FilterContainer filter = null;
            foreach (var child in node.Children.OfType<IElasticFilterNode>()) {
                var childFilter = child.Filter;
                var op = node.GetOperator(_config.DefaultFilterOperator);
                if (child.IsNegated())
                    childFilter = !childFilter;

                if (op == Operator.Or && !String.IsNullOrEmpty(node.Prefix) && node.Prefix == "+")
                    op = Operator.And;

                if (op == Operator.And) {
                    filter &= childFilter;
                } else if (op == Operator.Or) {
                    filter |= childFilter;
                }
            }

            node.Filter = filter;
            base.Visit(node);
        }
    }
}
