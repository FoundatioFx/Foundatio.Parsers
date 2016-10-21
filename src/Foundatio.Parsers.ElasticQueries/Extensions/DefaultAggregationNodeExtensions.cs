using System;
using Foundatio.Parsers.ElasticQueries.Visitors;
using Foundatio.Parsers.LuceneQueries.Extensions;
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
                            "date_" + node.GetUnaliasedField(),
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
                            "geogrid_" + node.GetUnaliasedField(),
                            new AggregationContainer {
                                GeoHash = new GeoHashAggregator {
                                    Field = node.Field,
                                    Precision = precision
                                },
                                Aggregations = {
                                    {
                                        "avg_lat",
                                        new AggregationContainer {
                                            Average = new AverageAggregator { Script = $"doc['{node.Field}'].lat" }
                                        }
                                    },
                                    {
                                        "avg_lon",
                                        new AggregationContainer {
                                            Average = new AverageAggregator { Script = $"doc['{node.Field}'].lon" }
                                        }
                                    }
                                }
                            }
                        );
                case AggregationType.Terms:
                    int? size = null;
                    int parsedSize;
                    if (!String.IsNullOrEmpty(node.Proximity) && Int32.TryParse(node.Proximity, out parsedSize))
                        size = parsedSize;

                    return new NamedAggregationContainer(
                            "terms_" + node.GetUnaliasedField(),
                            new AggregationContainer { Terms = new TermsAggregator { Field = node.Field, Size = size } }
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
                            "min_" + node.GetUnaliasedField(),
                            new AggregationContainer { Min = new MinAggregator { Field = node.Field } }
                        );
                case AggregationType.Max:
                    return new NamedAggregationContainer(
                            "max_" + node.GetUnaliasedField(),
                            new AggregationContainer { Max = new MaxAggregator { Field = node.Field } }
                        );
                case AggregationType.Avg:
                    return new NamedAggregationContainer(
                            "avg_" + node.GetUnaliasedField(),
                            new AggregationContainer { Average = new AverageAggregator { Field = node.Field } }
                        );
                case AggregationType.Sum:
                    return new NamedAggregationContainer(
                            "sum_" + node.GetUnaliasedField(),
                            new AggregationContainer { Sum = new SumAggregator { Field = node.Field } }
                        );
                case AggregationType.Cardinality:
                    return new NamedAggregationContainer(
                            "cardinality_" + node.GetUnaliasedField(),
                            new AggregationContainer { Cardinality = new CardinalityAggregator { Field = node.Field } }
                        );
                case AggregationType.Missing:
                    return new NamedAggregationContainer(
                            "missing_" + node.GetUnaliasedField(),
                            new AggregationContainer { Missing = new MissingAggregator { Field = node.Field } }
                        );
                case AggregationType.DateHistogram:
                    return new NamedAggregationContainer(
                            "date_" + node.GetUnaliasedField(),
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
                            "percentiles_" + node.GetUnaliasedField(),
                            new AggregationContainer { Percentiles = new PercentilesAggregator { Field = node.Field } }
                        );
                case AggregationType.GeoHashGrid:
                    var precision = GeoHashPrecision.Precision1;
                    if (!String.IsNullOrEmpty(node.Proximity))
                        Enum.TryParse(node.Proximity, out precision);

                    return new NamedAggregationContainer(
                            "geogrid_" + node.GetUnaliasedField(),
                            new AggregationContainer {
                                GeoHash = new GeoHashAggregator {
                                    Field = node.Field,
                                    Precision = precision,
                                },
                                Aggregations = {
                                    {
                                        "avg_lat",
                                        new AggregationContainer {
                                            Average = new AverageAggregator { Script = $"doc['{node.Field}'].lat" }
                                        }
                                    },
                                    {
                                        "cavg_lon",
                                        new AggregationContainer {
                                            Average = new AverageAggregator { Script = $"doc['{node.Field}'].lon" }
                                        }
                                    }
                                }
                            }
                        );
                case AggregationType.Terms:
                    int? size = null;
                    int parsedSize;
                    if (!String.IsNullOrEmpty(node.Proximity) && Int32.TryParse(node.Proximity, out parsedSize))
                        size = parsedSize;

                    return new NamedAggregationContainer(
                            "terms_" + node.GetUnaliasedField(),
                            new AggregationContainer { Terms = new TermsAggregator { Field = node.Field, Size = size } }
                        );
            }

            return null;
        }
    }
}
