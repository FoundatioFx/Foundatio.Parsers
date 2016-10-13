using System;
using Nest;

namespace Foundatio.Parsers.ElasticQueries.Visitors {
    public interface IElasticQueryVisitorContext {
        Operator DefaultOperator { get; set; }
        string DefaultField { get; set; }
        Func<string, IElasticType> GetFieldMappingFunc { get; set; }
        IElasticType GetFieldMapping(string field);
        bool IsFieldAnalyzed(string field);
        bool IsNestedFieldType(string field);
        bool IsGeoFieldType(string field);
    }
}