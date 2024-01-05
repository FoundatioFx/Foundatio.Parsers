using System.Collections.Generic;
using Foundatio.Parsers.LuceneQueries.Nodes;

namespace Foundatio.Parsers.LuceneQueries.Visitors;

public interface IQueryVisitorContext
{
    GroupOperator DefaultOperator { get; set; }
    string[] DefaultFields { get; set; }
    string QueryType { get; set; }
    IDictionary<string, object> Data { get; }
}
