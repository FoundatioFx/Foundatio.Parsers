using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Text;
using Foundatio.Parsers.LuceneQueries.Visitors;
using Foundatio.Parsers.SqlQueries.Extensions;
using Microsoft.EntityFrameworkCore.Metadata;
using Microsoft.Extensions.Primitives;

namespace Foundatio.Parsers.SqlQueries.Visitors;

public class SqlQueryVisitorContext : QueryVisitorContext, ISqlQueryVisitorContext
{
    public List<EntityFieldInfo> Fields { get; set; }
    public Action<SearchTerm> SearchTokenizer { get; set; } = static _ => { };
    public IEntityType EntityType { get; set; }
}

[DebuggerDisplay("{FullName} IsNumber: {IsNumber} IsMoney: {IsMoney} IsDate: {IsDate} IsBoolean: {IsBoolean} IsCollection: {IsCollection}")]
public class EntityFieldInfo
{
    public required string Name { get; init; }
    public required string FullName { get; init; }
    public bool IsNumber { get; set; }
    public bool IsMoney { get; set; }
    public bool IsDate { get; set; }
    public bool IsBoolean { get; set; }
    public bool IsCollection { get; set; }
    public bool IsNavigation { get; set; }
    public EntityFieldInfo Parent { get; set; }
    public IDictionary<string, object> Data { get; set; } = new Dictionary<string, object>();

    protected bool Equals(EntityFieldInfo other) => Name == other.Name;

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

    public override int GetHashCode() => (Name != null ? Name.GetHashCode() : 0);

    public (string fieldPrefix, string fieldSuffix) GetFieldPrefixAndSuffix()
    {
        var fieldTree = new List<EntityFieldInfo>();
        EntityFieldInfo current = Parent;
        while (current != null)
        {
            fieldTree.Add(current);
            current = current.Parent;
        }

        fieldTree.Reverse();

        var prefix = new StringBuilder();
        var suffix = new StringBuilder();
        foreach (var field in fieldTree) {
            if (field.IsCollection)
            {
                prefix.Append($"{field.Name}.Any(");
                suffix.Append(")");
            }
            else
            {
                prefix.Append(field.Name).Append(".");
            }
        };

        return (prefix.ToString(), suffix.ToString());
    }
}

public class SearchTerm
{
    public EntityFieldInfo FieldInfo { get; set; }
    public string Term { get; set; }
    public List<string> Tokens { get; set; }
    public SqlSearchOperator Operator { get; set; } = SqlSearchOperator.Contains;
}

public enum SqlSearchOperator { Equals, Contains, StartsWith }
