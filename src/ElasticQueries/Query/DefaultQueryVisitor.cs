using System;
using Foundatio.Parsers.ElasticQueries.Extensions;
using Foundatio.Parsers.LuceneQueries.Extensions;
using Foundatio.Parsers.LuceneQueries.Nodes;
using Foundatio.Parsers.LuceneQueries.Visitors;
using Nest;

namespace Foundatio.Parsers.ElasticQueries.Query {
    public class DefaultQueryVisitor : ChainableQueryVisitor {
        private readonly ElasticQueryParserConfiguration _config;

        public DefaultQueryVisitor(ElasticQueryParserConfiguration config) {
            _config = config;
        }

        public override void Visit(TermNode node, IQueryVisitorContext context) {
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

            node.SetDefaultQuery(query);
        }

        public override void Visit(TermRangeNode node, IQueryVisitorContext context) {
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

            node.SetDefaultQuery(range);
        }

        public override void Visit(ExistsNode node, IQueryVisitorContext context) {
            node.SetDefaultQuery(new FilteredQuery {
                Filter = new ExistsFilter { Field = node.GetFullName() }.ToContainer()
            });
        }

        public override void Visit(MissingNode node, IQueryVisitorContext context) {
            node.SetDefaultQuery(new FilteredQuery {
                Filter = new MissingFilter { Field = node.GetFullName() }.ToContainer()
            });
        }
    }
}
