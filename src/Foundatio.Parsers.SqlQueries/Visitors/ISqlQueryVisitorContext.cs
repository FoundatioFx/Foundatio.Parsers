using System;
using System.Collections.Generic;
using Foundatio.Parsers.LuceneQueries.Visitors;

namespace Foundatio.Parsers.SqlQueries.Visitors;

public interface ISqlQueryVisitorContext : IQueryVisitorContext
{
    List<EntityFieldInfo> Fields { get; set; }
    Action<SearchTerm> SearchTokenizer { get; set; }
}
