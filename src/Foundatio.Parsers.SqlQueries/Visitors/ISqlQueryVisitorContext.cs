using System;
using System.Collections.Generic;
using Foundatio.Parsers.LuceneQueries.Visitors;

namespace Foundatio.Parsers.SqlQueries.Visitors;

public interface ISqlQueryVisitorContext : IQueryVisitorContext
{
    List<EntityFieldInfo> Fields { get; set; }
    Func<string, string[]> Tokenizer { get; set; }
}
