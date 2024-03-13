using System.Collections.Generic;
using Foundatio.Parsers.LuceneQueries.Visitors;

namespace Foundatio.Parsers.SqlQueries.Visitors {
    public interface ISqlQueryVisitorContext : IQueryVisitorContext {
        List<FieldInfo> Fields { get; set; }
    }
}
