using System;
using System.Linq;
using Foundatio.Parsers.ElasticQueries.Extensions;
using Foundatio.Parsers.LuceneQueries.Extensions;
using Foundatio.Parsers.LuceneQueries.Nodes;
using Foundatio.Parsers.LuceneQueries.Visitors;
using Nest;

namespace Foundatio.Parsers.ElasticQueries.Filter {
    public class FilterContainerVisitor : ChainableQueryVisitor {
        private readonly ElasticQueryParserConfiguration _config;

        public FilterContainerVisitor(ElasticQueryParserConfiguration config) {
            _config = config;
        }

        public override void Visit(GroupNode node) {
            FilterContainer filter = null;
            foreach (var child in node.Children.OfType<IFieldQueryNode>()) {
                child.Accept(this);

                var childFilter = child.GetFilter();
                var op = node.GetOperator(_config.DefaultFilterOperator);
                if (child.IsNodeNegated())
                    childFilter = !childFilter;
                
                if (op == Operator.Or && !String.IsNullOrEmpty(node.Prefix) && node.Prefix == "+")
                    op = Operator.And;

                if (op == Operator.And) {
                    filter &= childFilter;
                } else if (op == Operator.Or) {
                    filter |= childFilter;
                }
            }

            node.SetFilter(filter);
        }

        public override void Visit(TermNode node) {
            if (node.GetFilter() != null)
                return;

            FilterContainer filter = null;
            if (_config.IsFieldAnalyzed(node.GetFullName())) {
                PlainQuery query;
                if (!node.IsQuotedTerm && node.UnescapedTerm.EndsWith("*")) {
                    query = new QueryStringQuery {
                        DefaultField = node.GetFullName() ?? _config.DefaultField,
                        AllowLeadingWildcard = false,
                        AnalyzeWildcard = true,
                        Query = node.UnescapedTerm
                    };
                } else {
                    var q = new MatchQuery {
                        Field = node.GetFullName() ?? _config.DefaultField,
                        Query = node.UnescapedTerm
                    };
                    if (node.IsQuotedTerm)
                        q.Type = "phrase";

                    query = q;
                }

                filter = new QueryFilter {
                    Query = query.ToContainer()
                };
            } else {
                filter = new TermFilter {
                    Field = node.GetFullName(),
                    Value = node.UnescapedTerm
                };
            }

            node.SetFilter(filter);
        }

        public override void Visit(TermRangeNode node) {
            if (node.GetFilter() != null)
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

            node.SetFilter(range);
        }

        public override void Visit(ExistsNode node) {
            if (node.GetFilter() != null)
                return;

            node.SetFilter(new ExistsFilter { Field = node.GetFullName() });
        }

        public override void Visit(MissingNode node) {
            if (node.GetFilter() != null)
                return;

            node.SetFilter(new MissingFilter { Field = node.GetFullName() });
        }
    }
}
