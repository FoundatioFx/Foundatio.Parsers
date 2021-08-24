using System;
using Foundatio.Parsers.LuceneQueries.Visitors;

namespace Foundatio.Parsers.ElasticQueries.Visitors {
    public interface IElasticQueryVisitorContext : IQueryVisitorContext {
        Lazy<string> DefaultTimeZone { get; set; }
        bool UseScoring { get; set; }
        ElasticMappingResolver MappingResolver { get; set; }
    }
}