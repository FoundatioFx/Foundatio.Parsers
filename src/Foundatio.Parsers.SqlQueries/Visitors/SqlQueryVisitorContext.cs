using System;
using System.Threading.Tasks;
using Foundatio.Parsers.LuceneQueries.Visitors;

namespace Foundatio.Parsers.SqlQueries.Visitors;

public class SqlQueryVisitorContext : QueryVisitorContext, ISqlQueryVisitorContext {
    public Func<Task<string>> DefaultTimeZone { get; set; }
    public bool UseScoring { get; set; }
    //public ElasticMappingResolver MappingResolver { get; set; }
}
