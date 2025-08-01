using System.Collections.Generic;
using System.Linq;
using Nest;

namespace Foundatio.Parsers.ElasticQueries.Extensions;

public static class SearchDescriptorExtensions
{
    public static SearchDescriptor<T> Aggregations<T>(this SearchDescriptor<T> descriptor, AggregationContainer aggregations) where T : class
    {
        return descriptor.Aggregations(f => CopyAggregations(f, aggregations.Aggregations));
    }

    public static AggregationContainerDescriptor<T> CopyAggregations<T>(
        AggregationContainerDescriptor<T> target,
        IDictionary<string, IAggregationContainer> sourceAggregations
    ) where T : class
    {
        foreach (var kvp in sourceAggregations)
        {
            string name = kvp.Key;
            var agg = kvp.Value;

            if (agg.Nested != null)
            {
                // Nested aggregation: copy nested path and inner aggregations recursively
                target.Nested(name, n => n
                    .Path(agg.Nested.Path)
                    .Aggregations(a => CopyAggregations(a, agg.Nested.Aggregations)));
            }
            else if (agg.Terms != null)
            {
                target.Terms(name, t =>
                {
                    // Copy field
                    if (agg.Terms.Field != null)
                        t.Field(agg.Terms.Field);

                    // Copy exclude
                    if (agg.Terms.Exclude != null)
                    {
                        if (agg.Terms.Exclude.Values != null && agg.Terms.Exclude.Values.Count() > 0)
                        {
                            t.Exclude([.. agg.Terms.Exclude.Values]);
                        }
                    }

                    // Copy Meta if present
                    if (agg.Meta != null)
                    {
                        t.Meta(d => {
                            foreach (var meta in agg.Terms.Meta)
                                d.Add(meta.Key, meta.Value);
                            return d;
                        });
                    }

                    return t;
                });
            }
            else if (agg.Max != null)
            {
                target.Max(name, m =>
                {
                    // Copy field
                    if (agg.Max.Field != null)
                        m.Field(agg.Max.Field);

                    // Copy Meta if present
                    if (agg.Max.Meta != null)
                    {
                        m.Meta(d => {
                            foreach (var meta in agg.Max.Meta)
                                d.Add(meta.Key, meta.Value);
                            return d;
                        });
                    }

                    return m;
                });
            }
        }

        return target;
    }

    public static SearchDescriptor<T> Sort<T>(this SearchDescriptor<T> descriptor, IEnumerable<ISort> sorts) where T : class
    {
        var searchRequest = descriptor as ISearchRequest;

        foreach (var sort in sorts)
        {
            if (searchRequest.Sort == null)
                searchRequest.Sort = new List<ISort>();

            searchRequest.Sort.Add(sort);
        }

        return descriptor;
    }
}
