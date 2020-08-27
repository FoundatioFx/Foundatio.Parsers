using System;
using Foundatio.Parsers.LuceneQueries.Visitors;
using Nest;

namespace Foundatio.Parsers.ElasticQueries.Visitors {
    public interface IElasticQueryVisitorContext : IQueryVisitorContext {
        string DefaultTimeZone { get; set; }
        bool UseScoring { get; set; }
        ElasticMappingResolver MappingResolver { get; set; }
    }
}