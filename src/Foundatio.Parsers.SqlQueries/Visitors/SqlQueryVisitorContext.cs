using System;
using System.Collections.Generic;
using System.Diagnostics;
using Foundatio.Parsers.LuceneQueries.Visitors;
using Microsoft.EntityFrameworkCore.Metadata;

namespace Foundatio.Parsers.SqlQueries.Visitors;

public class SqlQueryVisitorContext : QueryVisitorContext, ISqlQueryVisitorContext
{
    public List<EntityFieldInfo> Fields { get; set; }
    public Func<EntityFieldInfo, string, SearchTokenizeResult> Tokenizer { get; set; } = static (_, t) => new SearchTokenizeResult([t], SqlSearchOperator.Contains);
    public IEntityType EntityType { get; set; }
}

[DebuggerDisplay("{Field} IsNumber: {IsNumber} IsMoney: {IsMoney} IsDate: {IsDate} IsBoolean: {IsBoolean} IsCollection: {IsCollection}")]
public class EntityFieldInfo
{
    protected bool Equals(EntityFieldInfo other) => Field == other.Field;

    public override bool Equals(object obj)
    {
        if (obj is null)
        {
            return false;
        }

        if (ReferenceEquals(this, obj))
        {
            return true;
        }

        if (obj.GetType() != GetType())
        {
            return false;
        }

        return Equals((EntityFieldInfo)obj);
    }

    public override int GetHashCode() => (Field != null ? Field.GetHashCode() : 0);

    public string Field { get; set; }
    public bool IsNumber { get; set; }
    public bool IsMoney { get; set; }
    public bool IsDate { get; set; }
    public bool IsBoolean { get; set; }
    public bool IsCollection { get; set; }
    public IDictionary<string, object> Data { get; set; } = new Dictionary<string, object>();
}

public record SearchTokenizeResult(string[] Tokens, SqlSearchOperator Operator);
public enum SqlSearchOperator { Equals, Contains }
