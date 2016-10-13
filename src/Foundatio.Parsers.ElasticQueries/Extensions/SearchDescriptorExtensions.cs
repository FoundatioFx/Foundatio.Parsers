using System;
using Nest;

namespace Foundatio.Parsers.ElasticQueries.Extensions {
    public static class SearchDescriptorExtensions {
        public static SearchDescriptor<T> Aggregations<T>(this SearchDescriptor<T> descriptor, AggregationContainer aggregations) where T : class {
            descriptor.Aggregations(f => {
                ((IAggregationContainer)f).Aggregations = aggregations.Aggregations;
                return f;
            });

            return descriptor;
        }
    }
}
