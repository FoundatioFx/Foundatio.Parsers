using Exceptionless.DateTimeExtensions;
using Foundatio.Parsers.ElasticQueries.Visitors;
using Foundatio.Parsers.LuceneQueries.Extensions;
using Foundatio.Parsers.LuceneQueries.Nodes;
using Foundatio.Parsers.LuceneQueries.Visitors;
using Nest;
using System;

namespace Foundatio.Parsers.ElasticQueries.Extensions {

    public static class DefaultQueryNodeExtensions {

        public static QueryBase GetDefaultQuery(this IQueryNode node, IQueryVisitorContext context) {
            var termNode = node as TermNode;
            if (termNode != null)
                return termNode.GetDefaultQuery(context);

            var termRangeNode = node as TermRangeNode;
            if (termRangeNode != null)
                return termRangeNode.GetDefaultQuery(context);

            var existsNode = node as ExistsNode;
            if (existsNode != null)
                return existsNode.GetDefaultQuery(context);

            var missingNode = node as MissingNode;
            if (missingNode != null)
                return missingNode.GetDefaultQuery(context);

            return null;
        }

        public static QueryBase GetDefaultQuery(this TermNode node, IQueryVisitorContext context) {
            var elasticContext = context as IElasticQueryVisitorContext;
            if (elasticContext == null)
                throw new ArgumentException("Context must be of type IElasticQueryVisitorContext", nameof(context));

            QueryBase query = null;
            if (elasticContext.IsPropertyAnalyzed(node.GetFullName())) {
                if (!node.IsQuotedTerm && node.UnescapedTerm.EndsWith("*")) {
                    query = new QueryStringQuery {
                        DefaultField = node.GetFullName() ?? elasticContext.DefaultField,
                        AllowLeadingWildcard = false,
                        AnalyzeWildcard = true,
                        Query = node.UnescapedTerm
                    };
                } else {
                    QueryBase q;
                    if (node.IsQuotedTerm) {
                        q = new MatchPhraseQuery {
                            Field = node.GetFullName() ?? elasticContext.DefaultField,
                            Query = node.UnescapedTerm
                        };
                    } else {
                        q = new MatchQuery {
                            Field = node.GetFullName() ?? elasticContext.DefaultField,
                            Query = node.UnescapedTerm
                        };
                    }

                    query = q;
                }
            } else {
                query = new TermQuery {
                    Field = node.GetFullName(),
                    Value = node.UnescapedTerm
                };
            }

            return query;
        }

        public static QueryBase GetDefaultQuery(this TermRangeNode node, IQueryVisitorContext context) {
            var elasticContext = context as IElasticQueryVisitorContext;
            if (elasticContext == null)
                throw new ArgumentException("Context must be of type IElasticQueryVisitorContext", nameof(context));

            if (elasticContext.IsDatePropertyType(node.GetFullName())) {
                var timezone = GetString(context, "TimeZone");
                var range = new DateRangeQuery { Field = node.GetFullName(), TimeZone = timezone };
                if (!String.IsNullOrWhiteSpace(node.UnescapedMin)) {
                    if (node.MinInclusive.HasValue && !node.MinInclusive.Value)
                        range.GreaterThan = node.UnescapedMin;
                    else
                        range.GreaterThanOrEqualTo = node.UnescapedMin;
                }

                if (!String.IsNullOrWhiteSpace(node.UnescapedMax)) {
                    if (node.MaxInclusive.HasValue && !node.MaxInclusive.Value)
                        range.LessThan = node.UnescapedMax;
                    else
                        range.LessThanOrEqualTo = node.UnescapedMax;
                }

                return range;
            } else {
                var range = new TermRangeQuery { Field = node.GetFullName() };
                if (!String.IsNullOrWhiteSpace(node.UnescapedMin)) {
                    if (node.MinInclusive.HasValue && !node.MinInclusive.Value)
                        range.GreaterThan = node.UnescapedMin;
                    else
                        range.GreaterThanOrEqualTo = node.UnescapedMin;
                }

                if (!String.IsNullOrWhiteSpace(node.UnescapedMax)) {
                    if (node.MaxInclusive.HasValue && !node.MaxInclusive.Value)
                        range.LessThan = node.UnescapedMax;
                    else
                        range.LessThanOrEqualTo = node.UnescapedMax;
                }

                return range;
            }
        }

        public static QueryBase GetDefaultQuery(this ExistsNode node, IQueryVisitorContext context) {
            return new ExistsQuery { Field = node.GetFullName() };
        }

        public static QueryBase GetDefaultQuery(this MissingNode node, IQueryVisitorContext context) {
            return new BoolQuery {
                MustNot = new QueryContainer[] {
                    new ExistsQuery {
                        Field =  node.GetFullName()
                    }
                }
            };
        }

        private static string GetString(IQueryVisitorContext context, string key) {
            object value;
            if (context.Data.TryGetValue(key, out value) && value is string)
                return (string)value;

            return null;
        }
    }
}