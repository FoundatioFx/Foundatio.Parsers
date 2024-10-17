using System;
using System.Collections.Generic;
using System.Diagnostics;
using Foundatio.Parsers.LuceneQueries.Visitors;
using Microsoft.EntityFrameworkCore.Metadata;

namespace Foundatio.Parsers.SqlQueries.Visitors;

public class SqlQueryVisitorContext : QueryVisitorContext, ISqlQueryVisitorContext
{
    public List<EntityFieldInfo> Fields { get; set; }
    public Action<SearchTerm> Tokenizer { get; set; } = static _ => { };
    public IEntityType EntityType { get; set; }
}

[DebuggerDisplay("{Field} IsNumber: {IsNumber} IsMoney: {IsMoney} IsDate: {IsDate} IsBoolean: {IsBoolean} IsCollection: {IsCollection}")]
public class EntityFieldInfo
{
    public string Field { get; init; }
    public bool IsNumber { get; set; }
    public bool IsMoney { get; set; }
    public bool IsDate { get; set; }
    public bool IsBoolean { get; set; }
    public bool IsCollection { get; set; }
    public IDictionary<string, object> Data { get; set; } = new Dictionary<string, object>();

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
}

public class SearchTerm
{
    public EntityFieldInfo FieldInfo { get; set; }
    public string Term { get; set; }
    public List<string> Tokens { get; set; }
    public SqlSearchOperator Operator { get; set; } = SqlSearchOperator.Contains;
}

public enum SqlSearchOperator { Equals, Contains, StartsWith }
