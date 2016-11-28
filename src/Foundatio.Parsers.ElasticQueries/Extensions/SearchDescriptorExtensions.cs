using System.Collections.Generic;
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

        public static SearchDescriptor<T> Sort<T>(this SearchDescriptor<T> descriptor, IEnumerable<IFieldSort> sorts) where T : class {
            var searchRequest = descriptor as ISearchRequest;

            foreach (var sort in sorts) {
                if (searchRequest.Sort == null)
                    searchRequest.Sort = new List<KeyValuePair<PropertyPathMarker, ISort>>();

                searchRequest.Sort.Add(new KeyValuePair<PropertyPathMarker, ISort>(sort.Field, sort));
            }

            return descriptor;
        }
    }
}
