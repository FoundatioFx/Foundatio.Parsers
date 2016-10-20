using System;
using Foundatio.Parsers.ElasticQueries.Visitors;
using Foundatio.Parsers.LuceneQueries.Nodes;
using Foundatio.Parsers.LuceneQueries.Visitors;
using Nest;

namespace Foundatio.Parsers.ElasticQueries.Extensions {
    public static class DefaultAggregationNodeExtensions {
        public static NamedAggregationContainer GetDefaultAggregation(this IQueryNode node, IQueryVisitorContext context) {
            var groupNode = node as GroupNode;
            if (groupNode != null)
                return groupNode.GetDefaultAggregation(context);

            var termNode = node as TermNode;
            if (termNode != null)
                return termNode.GetDefaultAggregation(context);

            return null;
        }

        public static NamedAggregationContainer GetDefaultAggregation(this GroupNode node, IQueryVisitorContext context) {
            var elasticContext = context as IElasticQueryVisitorContext;
            if (elasticContext == null)
                throw new ArgumentException("Context must be of type IElasticQueryVisitorContext", nameof(context));

            if (!node.HasParens || String.IsNullOrEmpty(node.Field) || node.Left != null)
                return null;

            switch (node.GetAggregationType()) {
                case AggregationType.DateHistogram:
                    return new NamedAggregationContainer(
                            "date_" + node.Field,
                            new AggregationContainer {
                                DateHistogram = new DateHistogramAggregator {
                                    Field = node.Field,
                                    Interval = node.Proximity ?? "1d",
                                    Format = "date_optional_time",
                                    Offset = node.UnescapedBoost
                                }
                            }
                        );
                case AggregationType.GeoHashGrid:
                    var precision = GeoHashPrecision.Precision1;
                    if (!String.IsNullOrEmpty(node.Proximity))
                        Enum.TryParse(node.Proximity, out precision);

                    return new NamedAggregationContainer(
                            "geogrid_" + node.Field,
                            new AggregationContainer {
                                GeoHash = new GeoHashAggregator {
                                    Field = node.Field,
                                    Precision = precision
                                }
                            }
                        );
                case AggregationType.Terms:
                    return new NamedAggregationContainer(
                            "terms_" + node.Field,
                            new AggregationContainer { Terms = new TermsAggregator { Field = node.Field } }
                        );
            }

            return null;
        }

        public static NamedAggregationContainer GetDefaultAggregation(this TermNode node, IQueryVisitorContext context) {
            var elasticContext = context as IElasticQueryVisitorContext;
            if (elasticContext == null)
                throw new ArgumentException("Context must be of type IElasticQueryVisitorContext", nameof(context));

            switch (node.GetAggregationType()) {
                case AggregationType.Min:
                    return new NamedAggregationContainer(
                            "min_" + node.Field,
                            new AggregationContainer { Min = new MinAggregator { Field = node.Field } }
                        );
                case AggregationType.Max:
                    return new NamedAggregationContainer(
                            "max_" + node.Field,
                            new AggregationContainer { Max = new MaxAggregator { Field = node.Field } }
                        );
                case AggregationType.Avg:
                    return new NamedAggregationContainer(
                            "avg_" + node.Field,
                            new AggregationContainer { Average = new AverageAggregator { Field = node.Field } }
                        );
                case AggregationType.Sum:
                    return new NamedAggregationContainer(
                            "sum_" + node.Field,
                            new AggregationContainer { Sum = new SumAggregator { Field = node.Field } }
                        );
                case AggregationType.Cardinality:
                    return new NamedAggregationContainer(
                            "cardinality_" + node.Field,
                            new AggregationContainer { Cardinality = new CardinalityAggregator { Field = node.Field } }
                        );
                case AggregationType.Missing:
                    return new NamedAggregationContainer(
                            "missing_" + node.Field,
                            new AggregationContainer { Missing = new MissingAggregator { Field = node.Field } }
                        );
                case AggregationType.DateHistogram:
                    return new NamedAggregationContainer(
                            "date_" + node.Field,
                            new AggregationContainer {
                                DateHistogram = new DateHistogramAggregator {
                                    Field = node.Field,
                                    Interval = node.Proximity ?? "1d",
                                    Format = "date_optional_time",
                                    Offset = node.UnescapedBoost
                                }
                            }
                        );
                case AggregationType.Percentiles:
                    return new NamedAggregationContainer(
                            "percentiles_" + node.Field,
                            new AggregationContainer { Percentiles = new PercentilesAggregator { Field = node.Field } }
                        );
                case AggregationType.GeoHashGrid:
                    var precision = GeoHashPrecision.Precision1;
                    if (!String.IsNullOrEmpty(node.Proximity))
                        Enum.TryParse(node.Proximity, out precision);

                    return new NamedAggregationContainer(
                            "geogrid_" + node.Field,
                            new AggregationContainer {
                                GeoHash = new GeoHashAggregator {
                                    Field = node.Field,
                                    Precision = precision
                                }
                            }
                        );
                case AggregationType.Terms:
                    return new NamedAggregationContainer(
                            "terms_" + node.Field,
                            new AggregationContainer { Terms = new TermsAggregator { Field = node.Field } }
                        );
            }

            return null;
        }
    }
}
