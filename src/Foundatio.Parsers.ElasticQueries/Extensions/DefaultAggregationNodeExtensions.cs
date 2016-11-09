using System;
using System.Collections.Generic;
using Foundatio.Parsers.ElasticQueries.Visitors;
using Foundatio.Parsers.LuceneQueries.Extensions;
using Foundatio.Parsers.LuceneQueries.Nodes;
using Foundatio.Parsers.LuceneQueries.Visitors;
using Nest;
using System.Linq;

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

            string field = elasticContext.GetNonAnalyzedFieldName(node.Field);

            switch (node.GetAggregationType()) {
                case AggregationType.DateHistogram:
                    return new NamedAggregationContainer(
                            "date_" + node.GetOriginalField(),
                            new AggregationContainer {
                                DateHistogram = new DateHistogramAggregator {
                                    Field = field,
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

                    var latLonAverages = new Dictionary<string, IAggregationContainer> {
                                    {
                                        "avg_lat",
                                        new AggregationContainer {
                                            Average = new AverageAggregator { Script = $"doc['{field}'].lat" }
                                        }
                                    },
                                    {
                                        "avg_lon",
                                        new AggregationContainer {
                                            Average = new AverageAggregator { Script = $"doc['{field}'].lon" }
                                        }
                                    }
                                };

                    var geogridAgg = new NamedAggregationContainer(
                            "geogrid_" + node.GetOriginalField(),
                            new AggregationContainer {
                                GeoHash = new GeoHashAggregator {
                                    Field = field,
                                    Precision = precision
                                },
                                Aggregations = latLonAverages
                            }
                        );

                    return geogridAgg;
                case AggregationType.Terms:
                    int? size = null;
                    int parsedSize;
                    if (!String.IsNullOrEmpty(node.Proximity) && Int32.TryParse(node.Proximity, out parsedSize))
                        size = parsedSize;

                    return new NamedAggregationContainer(
                            "terms_" + node.GetOriginalField(),
                            new AggregationContainer { Terms = new TermsAggregator { Field = field, Size = size } }
                        );
            }

            return null;
        }

        public static NamedAggregationContainer GetDefaultAggregation(this TermNode node, IQueryVisitorContext context) {
            var elasticContext = context as IElasticQueryVisitorContext;
            if (elasticContext == null)
                throw new ArgumentException("Context must be of type IElasticQueryVisitorContext", nameof(context));

            string field = elasticContext.GetNonAnalyzedFieldName(node.Field);

            switch (node.GetAggregationType()) {
                case AggregationType.Min:
                    return new NamedAggregationContainer(
                            "min_" + node.GetOriginalField(),
                            new AggregationContainer { Min = new MinAggregator { Field = field } }
                        );
                case AggregationType.Max:
                    return new NamedAggregationContainer(
                            "max_" + node.GetOriginalField(),
                            new AggregationContainer { Max = new MaxAggregator { Field = field } }
                        );
                case AggregationType.Avg:
                    return new NamedAggregationContainer(
                            "avg_" + node.GetOriginalField(),
                            new AggregationContainer { Average = new AverageAggregator { Field = field } }
                        );
                case AggregationType.Sum:
                    return new NamedAggregationContainer(
                            "sum_" + node.GetOriginalField(),
                            new AggregationContainer { Sum = new SumAggregator { Field = field } }
                        );
                case AggregationType.Cardinality:
                    return new NamedAggregationContainer(
                            "cardinality_" + node.GetOriginalField(),
                            new AggregationContainer { Cardinality = new CardinalityAggregator { Field = field } }
                        );
                case AggregationType.Missing:
                    return new NamedAggregationContainer(
                            "missing_" + node.GetOriginalField(),
                            new AggregationContainer { Missing = new MissingAggregator { Field = field } }
                        );
                case AggregationType.DateHistogram:
                    return new NamedAggregationContainer(
                            "date_" + node.GetOriginalField(),
                            new AggregationContainer {
                                DateHistogram = new DateHistogramAggregator {
                                    Field = field,
                                    Interval = node.Proximity ?? "1d",
                                    Format = "date_optional_time",
                                    Offset = node.UnescapedBoost
                                }
                            }
                        );
                case AggregationType.Percentiles:
                    return new NamedAggregationContainer(
                            "percentiles_" + node.GetOriginalField(),
                            new AggregationContainer { Percentiles = new PercentilesAggregator { Field = field } }
                        );
                case AggregationType.GeoHashGrid:
                    var precision = GeoHashPrecision.Precision1;
                    if (!String.IsNullOrEmpty(node.Proximity))
                        Enum.TryParse(node.Proximity, out precision);

                    var latLonAverages = new Dictionary<string, IAggregationContainer> {
                                    {
                                        "avg_lat",
                                        new AggregationContainer {
                                            Average = new AverageAggregator { Script = $"doc['{field}'].lat" }
                                        }
                                    },
                                    {
                                        "avg_lon",
                                        new AggregationContainer {
                                            Average = new AverageAggregator { Script = $"doc['{field}'].lon" }
                                        }
                                    }
                                };

                    var geogridAgg = new NamedAggregationContainer(
                            "geogrid_" + node.GetOriginalField(),
                            new AggregationContainer {
                                GeoHash = new GeoHashAggregator {
                                    Field = field,
                                    Precision = precision
                                },
                                Aggregations = latLonAverages
                            }
                        );

                    return geogridAgg;
                case AggregationType.Terms:
                    int? size = null;
                    int parsedSize;
                    if (!String.IsNullOrEmpty(node.Proximity) && Int32.TryParse(node.Proximity, out parsedSize))
                        size = parsedSize;

                    return new NamedAggregationContainer(
                            "terms_" + node.GetOriginalField(),
                            new AggregationContainer { Terms = new TermsAggregator { Field = field, Size = size } }
                        );
            }

            return null;
        }
    }
}
