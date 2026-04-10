using System;
using System.Collections.Generic;
using System.Threading.Tasks;
using Foundatio.Parsers.LuceneQueries.Visitors;

namespace Foundatio.Parsers.ElasticQueries.Visitors;

public class ElasticQueryVisitorContext : QueryVisitorContext, IElasticQueryVisitorContext
{
    public Func<Task<string>>? DefaultTimeZone { get; set; }
    public bool UseScoring { get; set; }
    public ElasticMappingResolver MappingResolver { get; set; } = ElasticMappingResolver.NullInstance;
    public ICollection<ElasticRuntimeField> RuntimeFields { get; } = new List<ElasticRuntimeField>();
    public bool? EnableRuntimeFieldResolver { get; set; }
    public RuntimeFieldResolver? RuntimeFieldResolver { get; set; }
}
