using System.Collections.Generic;
using Elastic.Clients.Elasticsearch;
using Elastic.Clients.Elasticsearch.Aggregations;

namespace Foundatio.Parsers.ElasticQueries.Extensions;

public static class SearchDescriptorExtensions
{
    public static SearchRequestDescriptor<T> Aggregations<T>(this SearchRequestDescriptor<T> descriptor, Aggregation aggregations) where T : class
    {
        descriptor.Aggregations(f =>
        {
            ((Aggregation)f).Aggregations = aggregations.Aggregations;
            return f;
        });

        return descriptor;
    }

    public static SearchRequestDescriptor<T> Sort<T>(this SearchRequestDescriptor<T> descriptor, IEnumerable<SortOptions> sorts) where T : class
    {
        foreach (var sort in sorts)
        {
            if (descriptor.Sort == null)
                descriptor.Sort = new List<SortOptions>();

            descriptor.Sort.Add(sort);
        }

        return descriptor;
    }
}
