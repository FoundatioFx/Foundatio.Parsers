using System;
using System.Collections.Generic;
using System.Linq;
using Foundatio.Parsers.ElasticQueries.Visitors;
using Foundatio.Parsers.LuceneQueries.Extensions;
using Foundatio.Parsers.LuceneQueries.Nodes;
using Foundatio.Parsers.LuceneQueries.Visitors;
using Nest;

namespace Foundatio.Parsers.ElasticQueries.Extensions {

    public static class DefaultQueryNodeExtensions {

        public static QueryBase GetDefaultQuery(this IQueryNode node, IQueryVisitorContext context) {
            if (node is TermNode termNode)
                return termNode.GetDefaultQuery(context);

            if (node is TermRangeNode termRangeNode)
                return termRangeNode.GetDefaultQuery(context);

            if (node is ExistsNode existsNode)
                return existsNode.GetDefaultQuery(context);

            if (node is MissingNode missingNode)
                return missingNode.GetDefaultQuery(context);

            if (node is GroupNode groupNode)
                return groupNode.GetDefaultQuery(context);
            return null;
        }

        public static QueryBase GetDefaultQuery(this GroupNode node, IQueryVisitorContext context) {
            if (!(context is IElasticQueryVisitorContext elasticContext))
                throw new ArgumentException("Context must be of type IElasticQueryVisitorContext", nameof(context));

            if ((node.Parent == null || node.HasParens) && node is IFieldQueryNode) {
                if (node.Data.ContainsKey("match_terms")) {
                    var fields = !String.IsNullOrEmpty(node.GetFullName()) ? new[] { node.GetFullName() } : elasticContext.DefaultFields;
                    string values = String.Join(" ", ((List<TermNode>)node.Data["match_terms"]).Select(t => t.UnescapedTerm));
                    if (fields.Length == 1) {
                        return new MatchQuery {
                            Field = fields[0],
                            Query = values
                        };
                    }

                    return new MultiMatchQuery {
                        Fields = fields,
                        Query = values,
                        Type = TextQueryType.BestFields
                    };
                }
            }
            return null;
        }

        public static QueryBase GetDefaultQuery(this TermNode node, IQueryVisitorContext context) {
            if (!(context is IElasticQueryVisitorContext elasticContext))
                throw new ArgumentException("Context must be of type IElasticQueryVisitorContext", nameof(context));

            QueryBase query;
            string nodeFullName = node.GetFullName();
            if (elasticContext.IsPropertyAnalyzed(nodeFullName)) {
                var fields = !String.IsNullOrEmpty(nodeFullName) ? new[] { nodeFullName } : elasticContext.DefaultFields;

                if (!node.IsQuotedTerm && node.UnescapedTerm.EndsWith("*")) {
                    query = new QueryStringQuery {
                        Fields = fields,
                        AllowLeadingWildcard = false,
                        AnalyzeWildcard = true,
                        Query = node.UnescapedTerm
                    };
                } else {
                    if (fields.Length == 1) {
                        if (node.IsQuotedTerm) {
                            query = new MatchPhraseQuery {
                                Field = fields[0],
                                Query = node.UnescapedTerm
                            };
                        } else {
                            query = new MatchQuery {
                                Field = fields[0],
                                Query = node.UnescapedTerm
                            };
                        }
                    } else {
                        query = new MultiMatchQuery {
                            Fields = fields,
                            Query = node.UnescapedTerm
                        };
                        if (node.IsQuotedTerm)
                            (query as MultiMatchQuery).Type = TextQueryType.Phrase;
                    }
                }
            } else {
                query = new TermQuery {
                    Field = nodeFullName,
                    Value = node.UnescapedTerm
                };
            }

            return query;
        }

        public static QueryBase GetDefaultQuery(this TermRangeNode node, IQueryVisitorContext context) {
            if (!(context is IElasticQueryVisitorContext elasticContext))
                throw new ArgumentException("Context must be of type IElasticQueryVisitorContext", nameof(context));

            string nodeFullName = node.GetFullName();
            if (elasticContext.IsDatePropertyType(nodeFullName)) {
                string timezone = GetString(context, "TimeZone");
                var range = new DateRangeQuery { Field = nodeFullName, TimeZone = timezone };
                if (!String.IsNullOrWhiteSpace(node.UnescapedMin) && node.UnescapedMin != "*") {
                    if (node.MinInclusive.HasValue && !node.MinInclusive.Value)
                        range.GreaterThan = node.UnescapedMin;
                    else
                        range.GreaterThanOrEqualTo = node.UnescapedMin;
                }

                if (!String.IsNullOrWhiteSpace(node.UnescapedMax) && node.UnescapedMax != "*") {
                    if (node.MaxInclusive.HasValue && !node.MaxInclusive.Value)
                        range.LessThan = node.UnescapedMax;
                    else
                        range.LessThanOrEqualTo = node.UnescapedMax;
                }

                return range;
            } else {
                var range = new TermRangeQuery { Field = nodeFullName };
                if (!String.IsNullOrWhiteSpace(node.UnescapedMin) && node.UnescapedMin != "*") {
                    if (node.MinInclusive.HasValue && !node.MinInclusive.Value)
                        range.GreaterThan = node.UnescapedMin;
                    else
                        range.GreaterThanOrEqualTo = node.UnescapedMin;
                }

                if (!String.IsNullOrWhiteSpace(node.UnescapedMax) && node.UnescapedMax != "*") {
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
            if (context.Data.TryGetValue(key, out var value) && value is string)
                return (string)value;

            return null;
        }
    }
}