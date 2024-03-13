using System;
using System.Linq;
using System.Text;
using Foundatio.Parsers.LuceneQueries.Nodes;
using Foundatio.Parsers.SqlQueries.Visitors;

namespace Foundatio.Parsers.SqlQueries.Extensions;

public static class SqlNodeExtensions
{
    public static string ToSqlString(this GroupNode node, ISqlQueryVisitorContext context)
    {
        if (node.Left == null && node.Right == null)
            return String.Empty;

        var defaultOperator = context.DefaultOperator;

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
            builder.Append(node.Left is GroupNode groupNode ? groupNode.ToSqlString(context) : node.Left.ToSqlString(context));

        if (node.Left != null && node.Right != null)
        {
            if (op == GroupOperator.Or || (op == GroupOperator.Default && defaultOperator == GroupOperator.Or))
                builder.Append(" OR ");
            else if (node.Right != null)
                builder.Append(" AND ");
        }

        if (node.Right != null)
            builder.Append(node.Right is GroupNode groupNode ? groupNode.ToSqlString(context) : node.Right.ToSqlString(context));

        if (node.HasParens)
            builder.Append(")");

        if (node.Proximity != null)
            builder.Append("~" + node.Proximity);

        if (node.Boost != null)
            builder.Append("^" + node.Boost);

        return builder.ToString();
    }

    public static string ToSqlString(this ExistsNode node, ISqlQueryVisitorContext context)
    {
        if (String.IsNullOrEmpty(node.Field))
            throw new ArgumentException("Field is required for exists node queries.");

        var builder = new StringBuilder();

        if (node.IsNegated.HasValue && node.IsNegated.Value)
            builder.Append("NOT ");

        builder.Append(node.Field);
        builder.Append(" IS NOT NULL");

        return builder.ToString();
    }

    public static string ToSqlString(this MissingNode node, ISqlQueryVisitorContext context)
    {
        if (String.IsNullOrEmpty(node.Field))
            throw new ArgumentException("Field is required for missing node queries.");

        if (!String.IsNullOrEmpty(node.Prefix))
            throw new ArgumentException("Prefix is not supported for term range queries.");

        var builder = new StringBuilder();

        if (node.IsNegated.HasValue && node.IsNegated.Value)
            builder.Append("NOT ");

        builder.Append(node.Field);
        builder.Append(" IS NULL");

        return builder.ToString();
    }

    public static string ToSqlString(this TermNode node, ISqlQueryVisitorContext context)
    {
        if (String.IsNullOrEmpty(node.Field))
            throw new ArgumentException("Field is required for term node queries.");

        if (!String.IsNullOrEmpty(node.Prefix))
            throw new ArgumentException("Prefix is not supported for term range queries.");

        var builder = new StringBuilder();

        if (node.IsNegated.HasValue && node.IsNegated.Value)
            builder.Append("NOT ");

        builder.Append(node.Field);
        if (node.IsNegated.HasValue && node.IsNegated.Value)
            builder.Append(" != ");
        else
            builder.Append(" = ");

        // TODO: This needs to resolve the field recursively
        var field = context.Fields.FirstOrDefault(f => f.Field.Equals(node.Field, StringComparison.OrdinalIgnoreCase));
        if (field != null && (field.IsNumber || field.IsBoolean))
            builder.Append(node.Term);
        else
            builder.Append("\"" + node.Term + "\"");

        return builder.ToString();
    }

    public static string ToSqlString(this TermRangeNode node, ISqlQueryVisitorContext context)
    {
        if (String.IsNullOrEmpty(node.Field))
            throw new ArgumentException("Field is required for term range queries.");
        if (!String.IsNullOrEmpty(node.Boost))
            throw new ArgumentException("Boost is not supported for term range queries.");
        if (!String.IsNullOrEmpty(node.Proximity))
            throw new ArgumentException("Proximity is not supported for term range queries.");

        var builder = new StringBuilder();

        if (node.IsNegated.HasValue && node.IsNegated.Value)
            builder.Append("NOT ");

        if (node.Min != null && node.Max != null)
            builder.Append("(");

        if (node.Min != null)
        {
            builder.Append(node.Field);
            builder.Append(node.MinInclusive == true ? " >= " : " > ");
            builder.Append(node.Min);
        }

        if (node.Min != null && node.Max != null)
            builder.Append(" AND ");

        if (node.Max != null)
        {
            builder.Append(node.Field);
            builder.Append(node.MaxInclusive == true ? " <= " : " < ");
            builder.Append(node.Max);
        }

        if (node.Min != null && node.Max != null)
            builder.Append(")");

        return builder.ToString();
    }

    public static string ToSqlString(this IQueryNode node, ISqlQueryVisitorContext context)
    {
        return node switch
        {
            GroupNode groupNode => groupNode.ToSqlString(context),
            ExistsNode existsNode => existsNode.ToSqlString(context),
            MissingNode missingNode => missingNode.ToSqlString(context),
            TermNode termNode => termNode.ToSqlString(context),
            TermRangeNode termRangeNode => termRangeNode.ToSqlString(context),
            _ => throw new NotSupportedException($"Node type {node.GetType().Name} is not supported.")
        };
    }
}
