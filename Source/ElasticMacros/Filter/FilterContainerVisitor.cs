using System;
using System.Linq;
using ElasticQueryParser;
using Exceptionless.ElasticQueryParser.Extensions;
using Exceptionless.ElasticQueryParser.Filter.Nodes;
using Exceptionless.LuceneQueryParser.Extensions;
using Exceptionless.LuceneQueryParser.Nodes;
using Nest;

namespace Exceptionless.ElasticQueryParser.Filter {
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
                filter = new QueryFilter { Query = new QueryStringQuery {
                    Query = node.IsQuotedTerm ? "\"" + node.UnescapedTerm + "\"" : node.UnescapedTerm,
                    DefaultField = node.GetDefaultField(_config.DefaultField),
                    AllowLeadingWildcard = false,
                    AnalyzeWildcard = true,
                    DefaultOperator = node.GetOperator(_config.DefaultFilterOperator)
                }.ToContainer() };
            } else {
                filter = new TermFilter {
                    Field = node.GetDefaultField(_config.DefaultField),
                    Value = node.UnescapedTerm
                };
            }

            node.Filter = filter;
        }

        public override void Visit(FilterTermRangeNode node) {
            if (node.Filter != null)
                return;

            var range = new RangeFilter { Field = node.Field };
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

            node.Filter = new ExistsFilter { Field = node.Field };
        }

        public override void Visit(FilterMissingNode node) {
            if (node.Filter != null)
                return;

            node.Filter = new MissingFilter { Field = node.Field };
        }
    }
}
