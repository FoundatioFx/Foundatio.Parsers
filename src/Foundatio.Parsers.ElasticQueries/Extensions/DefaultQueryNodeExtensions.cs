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

            if ((node.Parent != null && !node.HasParens) || !(node is IFieldQueryNode))
                return null;
            
            if (!node.Data.ContainsKey("match_terms"))
                return null;
            
            var fields = !String.IsNullOrEmpty(node.Field) ? new[] { node.Field } : node.GetDefaultFields(elasticContext.DefaultFields);
            string values = String.Join(" ", ((List<TermNode>)node.Data["match_terms"]).Select(t => t.UnescapedTerm));
            if (fields == null || fields.Length == 1) {
                return new MatchQuery {
                    Field = fields?[0],
                    Query = values
                };
            }

            return new MultiMatchQuery {
                Fields = fields,
                Query = values,
                Type = TextQueryType.BestFields
            };
        }

        public static QueryBase GetDefaultQuery(this TermNode node, IQueryVisitorContext context) {
            if (!(context is IElasticQueryVisitorContext elasticContext))
                throw new ArgumentException("Context must be of type IElasticQueryVisitorContext", nameof(context));

            QueryBase query;
            string field = node.Field;
            var defaultFields = node.GetDefaultFields(elasticContext.DefaultFields);
            if (field == null && defaultFields.Length == 1)
                field = defaultFields[0];
            
            if (elasticContext.IsPropertyAnalyzed(field)) {
                var fields = !String.IsNullOrEmpty(field) ? new[] { field } : defaultFields;

                if (!node.IsQuotedTerm && node.UnescapedTerm.EndsWith("*")) {
                    query = new QueryStringQuery {
                        Fields = fields,
                        AllowLeadingWildcard = false,
                        AnalyzeWildcard = true,
                        Query = node.UnescapedTerm
                    };
                } else {
                    if (fields == null || fields.Length == 1) {
                        if (node.IsQuotedTerm) {
                            query = new MatchPhraseQuery {
                                Field = fields?[0],
                                Query = node.UnescapedTerm
                            };
                        } else {
                            query = new MatchQuery {
                                Field = fields?[0],
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
                    Field = field,
                    Value = node.UnescapedTerm
                };
            }

            return query;
        }

        public static QueryBase GetDefaultQuery(this TermRangeNode node, IQueryVisitorContext context) {
            if (!(context is IElasticQueryVisitorContext elasticContext))
                throw new ArgumentException("Context must be of type IElasticQueryVisitorContext", nameof(context));

            string field = node.Field;
            if (elasticContext.IsDatePropertyType(field)) {
                string timezone = GetString(context, "TimeZone");
                var range = new DateRangeQuery { Field = field, TimeZone = timezone };
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
                var range = new TermRangeQuery { Field = field };
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
            return new ExistsQuery { Field = node.Field };
        }

        public static QueryBase GetDefaultQuery(this MissingNode node, IQueryVisitorContext context) {
            return new BoolQuery {
                MustNot = new QueryContainer[] {
                    new ExistsQuery {
                        Field =  node.Field
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