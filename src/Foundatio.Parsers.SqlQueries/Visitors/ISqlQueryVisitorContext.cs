using System;
using System.Threading.Tasks;
using Foundatio.Parsers.LuceneQueries.Visitors;

namespace Foundatio.Parsers.SqlQueries.Visitors {
    public interface ISqlQueryVisitorContext : IQueryVisitorContext {
        Func<Task<string>> DefaultTimeZone { get; set; }
        bool UseScoring { get; set; }
        //ElasticMappingResolver MappingResolver { get; set; }
    }
}
