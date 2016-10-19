using System;
using Foundatio.Parsers.LuceneQueries.Visitors;
using Nest;

namespace Foundatio.Parsers.ElasticQueries.Visitors {
    public interface IElasticQueryVisitorContext : IQueryVisitorContext {
        Operator DefaultOperator { get; set; }
        string DefaultField { get; set; }
        Func<string, IElasticType> GetFieldMappingFunc { get; set; }
    }
}