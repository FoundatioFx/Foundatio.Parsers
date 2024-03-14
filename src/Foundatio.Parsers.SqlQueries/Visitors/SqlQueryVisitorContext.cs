using System.Collections.Generic;
using System.Diagnostics;
using Foundatio.Parsers.LuceneQueries.Visitors;

namespace Foundatio.Parsers.SqlQueries.Visitors;

public class SqlQueryVisitorContext : QueryVisitorContext, ISqlQueryVisitorContext {
    public List<FieldInfo> Fields { get; set; }
}

[DebuggerDisplay("{Field} IsNumber: {IsNumber} IsDate: {IsDate} IsBoolean: {IsBoolean} Children: {Children?.Count}")]
public class FieldInfo
{
    public string Field { get; set; }
    public bool IsNumber { get; set; }
    public bool IsDate { get; set; }
    public bool IsBoolean { get; set; }
    public IDictionary<string, object> Data { get; set; } = new Dictionary<string, object>();
}
