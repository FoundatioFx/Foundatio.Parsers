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
        foreach (var kvp in sourceAggregations.OrderBy(x => x.Key))
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
                    {
                        var fieldName = agg.Terms.Field.Name;

                        // For text fields, use the keyword sub-field if it's not already specified
                        // This helps handle the common case where a text field needs to be aggregated
                        bool isTextFieldWithoutKeyword = agg.Meta != null &&
                                                      agg.Meta.TryGetValue("@field_type", out var fieldType) &&
                                                      fieldType?.ToString() == "text" &&
                                                      !fieldName.EndsWith(".keyword");

                        if (isTextFieldWithoutKeyword)
                        {
                            // Use the keyword sub-field for text field aggregations
                            t.Field($"{fieldName}.keyword");
                        }
                        else
                        {
                            t.Field(agg.Terms.Field);
                        }
                    }

                    // Copy exclude
                    if (agg.Terms.Exclude != null)
                    {
                        if (agg.Terms.Exclude.Values != null && agg.Terms.Exclude.Values.Any())
                        {
                            t.Exclude([.. agg.Terms.Exclude.Values.OrderBy(v => v)]);
                        }
                    }

                    // Copy include
                    if (agg.Terms.Include != null)
                    {
                        if (agg.Terms.Include.Values != null && agg.Terms.Include.Values.Any())
                        {
                            t.Include([.. agg.Terms.Include.Values.OrderBy(v => v)]);
                        }
                    }

                    // Copy Meta if present
                    if (agg.Meta != null && agg.Terms.Meta !=null)
                    {
                        t.Meta(d => {
                            foreach (var meta in agg.Terms.Meta.OrderBy(m => m.Key))
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
                            foreach (var meta in agg.Max.Meta.OrderBy(m => m.Key))
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
