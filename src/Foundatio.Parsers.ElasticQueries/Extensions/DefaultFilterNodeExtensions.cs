using System;
using Foundatio.Parsers.ElasticQueries.Visitors;
using Foundatio.Parsers.LuceneQueries.Extensions;
using Foundatio.Parsers.LuceneQueries.Nodes;
using Foundatio.Parsers.LuceneQueries.Visitors;
using Nest;

namespace Foundatio.Parsers.ElasticQueries.Extensions {
    public static class DefaultFilterNodeExtensions {
        public static PlainFilter GetDefaultFilter(this IQueryNode node, IQueryVisitorContext context) {
            var termNode = node as TermNode;
            if (termNode != null)
                return termNode.GetDefaultFilter(context);

            var termRangeNode = node as TermRangeNode;
            if (termRangeNode != null)
                return termRangeNode.GetDefaultFilter(context);

            var existsNode = node as ExistsNode;
            if (existsNode != null)
                return existsNode.GetDefaultFilter(context);


            var missingNode = node as MissingNode;
            if (missingNode != null)
                return missingNode.GetDefaultFilter(context);

            return null;
        }


        public static PlainFilter GetDefaultFilter(this TermNode node, IQueryVisitorContext context) {
            var elasticContext = context as IElasticQueryVisitorContext;
            if (elasticContext == null)
                throw new ArgumentException("Context must be of type ElasticQueryVisitorContext", nameof(context));

            PlainFilter filter = null;
            if (elasticContext.IsFieldAnalyzed(node.GetFullName())) {
                PlainQuery query;
                if (!node.IsQuotedTerm && node.UnescapedTerm.EndsWith("*")) {
                    query = new QueryStringQuery {
                        DefaultField = node.GetFullName() ?? elasticContext.DefaultField,
                        AllowLeadingWildcard = false,
                        AnalyzeWildcard = true,
                        Query = node.UnescapedTerm
                    };
                } else {
                    var q = new MatchQuery {
                        Field = node.GetFullName() ?? elasticContext.DefaultField,
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

            return filter;
        }

        public static PlainFilter GetDefaultFilter(this TermRangeNode node, IQueryVisitorContext context) {
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

            return range;
        }

        public static PlainFilter GetDefaultFilter(this ExistsNode node, IQueryVisitorContext context) {
            return new ExistsFilter { Field = node.GetFullName() };
        }

        public static PlainFilter GetDefaultFilter(this MissingNode node, IQueryVisitorContext context) {
            return new MissingFilter { Field = node.GetFullName() };
        }
    }
}
