using System;
using System.Linq;
using Foundatio.Parsers.ElasticQueries.Extensions;
using Foundatio.Parsers.ElasticQueries.Filter.Nodes;
using Foundatio.Parsers.LuceneQueries.Extensions;
using Nest;

namespace Foundatio.Parsers.ElasticQueries.Filter {
    public class FilterContainerVisitor : ElasticFilterNodeVisitorBase {
        private readonly ElasticQueryParserConfiguration _config;

        public FilterContainerVisitor(ElasticQueryParserConfiguration config) {
            _config = config;
        }

        public override void Visit(FilterGroupNode node) {
            FilterContainer filter = null;
            foreach (var child in node.Children.OfType<IElasticFilterNode>()) {
                child.Accept(this);

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
        }

        public override void Visit(FilterTermNode node) {
            if (node.Filter != null)
                return;

            FilterContainer filter = null;
            if (_config.IsFieldAnalyzed(node.GetFullName())) {
                var q = new MatchQuery {
                    Field = node.GetFullName() ?? _config.DefaultField,
                    Query = node.UnescapedTerm
                };
                if (node.IsQuotedTerm)
                    q.Type = "phrase";

                filter = new QueryFilter {
                    Query = q.ToContainer()
                };
            } else {
                filter = new TermFilter {
                    Field = node.GetFullName(),
                    Value = node.UnescapedTerm
                };
            }

            node.Filter = filter;
        }

        public override void Visit(FilterTermRangeNode node) {
            if (node.Filter != null)
                return;

            var range = new RangeFilter { Field = node.GetFullName() };
            if (!String.IsNullOrWhiteSpace(node.UnescapedMin)) {
                if (node.MinInclusive.HasValue && !node.MinInclusive.Value)
                    range.GreaterThan = node.UnescapedMin;
                else
                    range.GreaterThanOrEqualTo = node.UnescapedMin;
            }

            if (!String.IsNullOrWhiteSpace(node.UnescapedMax)) {
                if (node.MaxInclusive.HasValue && !node.MaxInclusive.Value)
                    range.LowerThan = node.UnescapedMax;
                else
                    range.LowerThanOrEqualTo = node.UnescapedMax;
            }

            node.Filter = range;
        }

        public override void Visit(FilterExistsNode node) {
            if (node.Filter != null)
                return;

            node.Filter = new ExistsFilter { Field = node.GetFullName() };
        }

        public override void Visit(FilterMissingNode node) {
            if (node.Filter != null)
                return;

            node.Filter = new MissingFilter { Field = node.GetFullName() };
        }
    }
}
