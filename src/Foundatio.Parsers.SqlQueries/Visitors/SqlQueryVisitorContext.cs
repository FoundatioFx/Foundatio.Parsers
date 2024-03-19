using System.Collections.Generic;
using System.Diagnostics;
using Foundatio.Parsers.LuceneQueries.Visitors;
using Microsoft.EntityFrameworkCore.Metadata;

namespace Foundatio.Parsers.SqlQueries.Visitors;

public class SqlQueryVisitorContext : QueryVisitorContext, ISqlQueryVisitorContext {
    public List<EntityFieldInfo> Fields { get; set; }
    public IEntityType EntityType { get; set; }
}

[DebuggerDisplay("{Field} IsNumber: {IsNumber} IsDate: {IsDate} IsBoolean: {IsBoolean} IsCollection: {IsCollection}")]
public class EntityFieldInfo
{
    public string Field { get; set; }
    public bool IsNumber { get; set; }
    public bool IsDate { get; set; }
    public bool IsBoolean { get; set; }
    public bool IsCollection { get; set; }
    public IDictionary<string, object> Data { get; set; } = new Dictionary<string, object>();
}
