using System;
using System.Linq;
using Foundatio.Parsers.ElasticQueries.Extensions;
using Foundatio.Parsers.LuceneQueries.Extensions;
using Foundatio.Parsers.LuceneQueries.Nodes;
using Foundatio.Parsers.LuceneQueries.Visitors;
using Nest;

namespace Foundatio.Parsers.ElasticQueries.Query {
    public class QueryContainerVisitor : ChainableQueryVisitor {
        private readonly ElasticQueryParserConfiguration _config;

        public QueryContainerVisitor(ElasticQueryParserConfiguration config) {
            _config = config;
        }

        public override void Visit(GroupNode node) {
            QueryContainer query = null;
            foreach (var child in node.Children.OfType<IFieldQueryNode>()) {
                child.Accept(this);

                var childQuery = child.GetQuery();
                var op = node.GetOperator(_config.DefaultQueryOperator);
                if (child.IsNodeNegated())
                    childQuery = !childQuery;

                if (op == Operator.Or && !String.IsNullOrEmpty(node.Prefix) && node.Prefix == "+")
                    op = Operator.And;

                if (op == Operator.And) {
                    query &= childQuery;
                } else if (op == Operator.Or) {
                    query |= childQuery;
                }
            }

            node.SetQuery(query);
        }

        public override void Visit(TermNode node) {
            PlainQuery query = null;
            if (_config.IsFieldAnalyzed(node.GetFullName())) {
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
            } else {
                query = new TermQuery {
                    Field = node.GetFullName(),
                    Value = node.UnescapedTerm
                };
            }

            node.SetQuery(query);
        }

        public override void Visit(TermRangeNode node) {
            var range = new RangeQuery { Field = node.GetFullName() };
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

            node.SetQuery(range);
        }

        public override void Visit(ExistsNode node) {
            node.SetQuery(new FilteredQuery {
                Filter = new ExistsFilter { Field = node.GetFullName() }.ToContainer()
            });
        }

        public override void Visit(MissingNode node) {
            node.SetQuery(new FilteredQuery {
                Filter = new MissingFilter { Field = node.GetFullName() }.ToContainer()
            });
        }
    }
}
