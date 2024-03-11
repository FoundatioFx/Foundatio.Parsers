using System;
using System.Text;
using Foundatio.Parsers.LuceneQueries.Nodes;

namespace Foundatio.Parsers.SqlQueries.Visitors;

public static class SqlNodeExtensions
{
    public static string ToSqlString(this GroupNode node, GroupOperator defaultOperator = GroupOperator.Default)
    {
        if (node.Left == null && node.Right == null)
            return String.Empty;

        var builder = new StringBuilder();
        var op = node.Operator != GroupOperator.Default ? node.Operator : defaultOperator;

        if (node.IsNegated.HasValue && node.IsNegated.Value)
            builder.Append("NOT ");

        builder.Append(node.Prefix);

        if (!String.IsNullOrEmpty(node.Field))
            builder.Append(node.Field).Append(':');

        if (node.HasParens)
            builder.Append("(");

        if (node.Left != null)
            builder.Append(node.Left is GroupNode groupNode ? groupNode.ToSqlString(defaultOperator) : node.Left.ToSqlString());

        if (node.Left != null && node.Right != null)
        {
            if (op == GroupOperator.And)
                builder.Append(" AND ");
            else if (op == GroupOperator.Or)
                builder.Append(" OR ");
            else if (node.Right != null)
                builder.Append(" ");
        }

        if (node.Right != null)
            builder.Append(node.Right is GroupNode groupNode ? groupNode.ToSqlString(defaultOperator) : node.Right.ToSqlString());

        if (node.HasParens)
            builder.Append(")");

        if (node.Proximity != null)
            builder.Append("~" + node.Proximity);

        if (node.Boost != null)
            builder.Append("^" + node.Boost);

        return builder.ToString();
    }

    public static string ToSqlString(this ExistsNode node)
    {
        var builder = new StringBuilder();

        if (node.IsNegated.HasValue && node.IsNegated.Value)
            builder.Append("NOT ");

        builder.Append(node.Prefix);
        builder.Append("_exists_");
        builder.Append(":");
        builder.Append(node.Field);

        return builder.ToString();
    }

    public static string ToSqlString(this MissingNode node)
    {
        var builder = new StringBuilder();

        if (node.IsNegated.HasValue && node.IsNegated.Value)
            builder.Append("NOT ");

        builder.Append(node.Prefix);
        builder.Append("_missing_");
        builder.Append(":");
        builder.Append(node.Field);

        return builder.ToString();
    }

    public static string ToSqlString(this TermNode node)
    {
        var builder = new StringBuilder();

        if (node.IsNegated.HasValue && node.IsNegated.Value)
            builder.Append("NOT ");

        if (!String.IsNullOrEmpty(node.Field))
        {
            builder.Append(node.Field);
            if (node.IsNegated.HasValue && node.IsNegated.Value)
                builder.Append(" != ");
            else
                builder.Append(" = ");
        }

        builder.Append("\"" + node.Term + "\"");

        return builder.ToString();
    }

    public static string ToSqlString(this TermRangeNode node)
    {
        var builder = new StringBuilder();

        if (node.IsNegated.HasValue && node.IsNegated.Value)
            builder.Append("NOT ");

        builder.Append(node.Prefix);

        if (!String.IsNullOrEmpty(node.Field))
        {
            builder.Append(node.Field);
            builder.Append(":");
        }

        if (!String.IsNullOrEmpty(node.Operator))
            builder.Append(node.Operator);

        if (node.MinInclusive.HasValue && String.IsNullOrEmpty(node.Operator))
            builder.Append(node.MinInclusive.Value ? "[" : "{");

        if (node.Min != null)
            builder.Append(node.Min);

        if (node.Delimiter != null)
            builder.Append(node.Delimiter);

        if (node.Max != null)
            builder.Append(node.Max);

        if (node.MaxInclusive.HasValue && String.IsNullOrEmpty(node.Operator))
            builder.Append(node.MaxInclusive.Value ? "]" : "}");

        if (node.Boost != null)
            builder.Append("^" + node.Boost);

        if (node.Proximity != null)
            builder.Append("~" + node.Proximity);

        return builder.ToString();
    }

    public static string ToSqlString(this IQueryNode node, GroupOperator defaultOperator = GroupOperator.Default)
    {
        return node switch
        {
            GroupNode groupNode => groupNode.ToSqlString(defaultOperator),
            ExistsNode existsNode => existsNode.ToSqlString(),
            MissingNode missingNode => missingNode.ToSqlString(),
            TermNode termNode => termNode.ToSqlString(),
            TermRangeNode termRangeNode => termRangeNode.ToSqlString(),
            _ => throw new NotSupportedException($"Node type {node.GetType().Name} is not supported.")
        };
    }
}
