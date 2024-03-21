using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using Foundatio.Parsers.LuceneQueries.Extensions;
using Foundatio.Parsers.LuceneQueries.Nodes;
using Foundatio.Parsers.SqlQueries.Visitors;

namespace Foundatio.Parsers.SqlQueries.Extensions;

public static class SqlNodeExtensions
{
    public static string ToDynamicLinqString(this GroupNode node, ISqlQueryVisitorContext context)
    {
        // support overriding the generated query
        if (node.TryGetQuery(out string query))
            return query;

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
            builder.Append(node.Left is GroupNode groupNode ? groupNode.ToDynamicLinqString(context) : node.Left.ToDynamicLinqString(context));

        if (node.Left != null && node.Right != null)
        {
            if (op == GroupOperator.Or || (op == GroupOperator.Default && defaultOperator == GroupOperator.Or))
                builder.Append(" OR ");
            else if (node.Right != null)
                builder.Append(" AND ");
        }

        if (node.Right != null)
            builder.Append(node.Right is GroupNode groupNode ? groupNode.ToDynamicLinqString(context) : node.Right.ToDynamicLinqString(context));

        if (node.HasParens)
            builder.Append(")");

        if (node.Proximity != null)
            builder.Append("~" + node.Proximity);

        if (node.Boost != null)
            builder.Append("^" + node.Boost);

        return builder.ToString();
    }

    public static string ToDynamicLinqString(this ExistsNode node, ISqlQueryVisitorContext context)
    {
        if (String.IsNullOrEmpty(node.Field))
            context.AddValidationError("Field is required for exists node queries.");

        // support overriding the generated query
        if (node.TryGetQuery(out string query))
            return query;

        var builder = new StringBuilder();

        builder.Append(node.Field);
        if (!node.IsNegated.HasValue || !node.IsNegated.Value)
            builder.Append(" != null");
        else
            builder.Append(" == null");

        return builder.ToString();
    }

    public static string ToDynamicLinqString(this MissingNode node, ISqlQueryVisitorContext context)
    {
        if (String.IsNullOrEmpty(node.Field))
            context.AddValidationError("Field is required for missing node queries.");

        if (!String.IsNullOrEmpty(node.Prefix))
            context.AddValidationError("Prefix is not supported for term range queries.");

        // support overriding the generated query
        if (node.TryGetQuery(out string query))
            return query;

        var builder = new StringBuilder();

        builder.Append(node.Field);
        if (!node.IsNegated.HasValue || !node.IsNegated.Value)
            builder.Append(" == null");
        else
            builder.Append(" != null");

        return builder.ToString();
    }

    public static EntityFieldInfo GetFieldInfo(List<EntityFieldInfo> fields, string field)
    {
        if (fields == null)
            return new EntityFieldInfo { Field = field };

        return fields.FirstOrDefault(f => f.Field.Equals(field, StringComparison.OrdinalIgnoreCase)) ??
               new EntityFieldInfo { Field = field };
    }

    public static string ToDynamicLinqString(this TermNode node, ISqlQueryVisitorContext context)
    {
        if (!String.IsNullOrEmpty(node.Prefix))
            context.AddValidationError("Prefix is not supported for term range queries.");

        var builder = new StringBuilder();

        if (String.IsNullOrEmpty(node.Field))
        {
            if (context.DefaultFields == null || context.DefaultFields.Length == 0)
            {
                context.AddValidationError("Field or DefaultFields is required for term queries.");
                return String.Empty;
            }

            for (int index = 0; index < context.DefaultFields.Length; index++)
            {
                builder.Append(index == 0 ? "(" : " OR ");

                var defaultField = GetFieldInfo(context.Fields, context.DefaultFields[index]);
                if (defaultField.IsCollection)
                {
                    var dotIndex = defaultField.Field.LastIndexOf('.');
                    var collectionField = defaultField.Field.Substring(0, dotIndex);
                    var fieldName = defaultField.Field.Substring(dotIndex + 1);

                    builder.Append(collectionField);
                    builder.Append(".Any(");
                    builder.Append(fieldName);
                    builder.Append(".Contains(\"").Append(node.Term).Append("\")");
                    builder.Append(")");
                }
                else
                {
                    builder.Append(defaultField.Field).Append(".Contains(\"").Append(node.Term).Append("\")");
                }

                if (index == context.DefaultFields.Length - 1)
                    builder.Append(")");
            }

            return builder.ToString();
        }

        // support overriding the generated query
        if (node.TryGetQuery(out string query))
            return query;

        var field = GetFieldInfo(context.Fields, node.Field);

        if (node.IsNegated.HasValue && node.IsNegated.Value)
            builder.Append("!");

        if (field.IsCollection)
        {
            var index = node.Field.LastIndexOf('.');
            var collectionField = node.Field.Substring(0, index);
            var fieldName = node.Field.Substring(index + 1);

            builder.Append(collectionField);
            builder.Append(".Any(");
            builder.Append(fieldName);

            if (node.IsNegated.HasValue && node.IsNegated.Value)
                builder.Append(" != ");
            else
                builder.Append(" = ");

            AppendField(builder, field, node.Term);

            builder.Append(")");
        }
        else
        {
            builder.Append(node.Field);
            if (node.IsNegated.HasValue && node.IsNegated.Value)
                builder.Append(" != ");
            else
                builder.Append(" = ");

            AppendField(builder, field, node.Term);
        }

        return builder.ToString();
    }

    private static void AppendField(StringBuilder builder, EntityFieldInfo field, string term)
    {
        if (field == null)
            return;

        if (field.IsNumber || field.IsBoolean || field.IsMoney)
            builder.Append(term);
        else if (field is { IsDate: true })
        {
            term = term.Trim();
            if (term.StartsWith("now", StringComparison.OrdinalIgnoreCase))
            {
                builder.Append("DateTime.UtcNow");

                if (term.Length == 3)
                    return;

                builder.Append(".");

                var method = term[^1..] switch {
                    "y" => "AddYears",
                    "M" => "AddMonths",
                    "d" => "AddDays",
                    "h" => "AddHours",
                    "H" => "AddHours",
                    "m" => "AddMinutes",
                    "s" => "AddSeconds",
                    _ => throw new NotSupportedException("Invalid date operation.")
                };

                var subtract = term.Substring(3, 1) == "-";

                builder.Append(method).Append("(").Append(subtract ? "-" : "").Append(term.Substring(4, term.Length - 5)).Append(")");
            }
            else
            {
                builder.Append("DateTime.Parse(\"" + term + "\")");
            }
        }
        else
            builder.Append("\"" + term + "\"");
    }

    public static string ToDynamicLinqString(this TermRangeNode node, ISqlQueryVisitorContext context)
    {
        if (String.IsNullOrEmpty(node.Field))
            context.AddValidationError("Field is required for term range queries.");
        if (!String.IsNullOrEmpty(node.Boost))
            context.AddValidationError("Boost is not supported for term range queries.");
        if (!String.IsNullOrEmpty(node.Proximity))
            context.AddValidationError("Proximity is not supported for term range queries.");

        // support overriding the generated query
        if (node.TryGetQuery(out string query))
            return query;

        var field = GetFieldInfo(context.Fields, node.Field);
        if (!field.IsNumber && !field.IsDate && !field.IsMoney)
            context.AddValidationError("Field must be a number, money or date for term range queries.");

        var builder = new StringBuilder();

        if (node.IsNegated.HasValue && node.IsNegated.Value)
            builder.Append("NOT ");

        if (node.Min != null && node.Max != null)
            builder.Append("(");

        if (node.Min != null)
        {
            builder.Append(node.Field);
            builder.Append(node.MinInclusive == true ? " >= " : " > ");
            AppendField(builder, field, node.Min);
        }

        if (node.Min != null && node.Max != null)
            builder.Append(" AND ");

        if (node.Max != null)
        {
            builder.Append(node.Field);
            builder.Append(node.MaxInclusive == true ? " <= " : " < ");
            AppendField(builder, field, node.Max);
        }

        if (node.Min != null && node.Max != null)
            builder.Append(")");

        return builder.ToString();
    }

    public static string ToDynamicLinqString(this IQueryNode node, ISqlQueryVisitorContext context)
    {
        return node switch
        {
            GroupNode groupNode => groupNode.ToDynamicLinqString(context),
            ExistsNode existsNode => existsNode.ToDynamicLinqString(context),
            MissingNode missingNode => missingNode.ToDynamicLinqString(context),
            TermNode termNode => termNode.ToDynamicLinqString(context),
            TermRangeNode termRangeNode => termRangeNode.ToDynamicLinqString(context),
            _ => throw new NotSupportedException($"Node type {node.GetType().Name} is not supported.")
        };
    }

    private const string QueryKey = "Query";
    public static void SetQuery(this IQueryNode node, string query)
    {
        node.Data[QueryKey] = query;
    }

    public static string GetQuery(this IQueryNode node)
    {
        return node.Data.TryGetValue(QueryKey, out object query) ? query as string : null;
    }

    public static bool TryGetQuery(this IQueryNode node, out string query)
    {
        query = null;
        return node.Data.TryGetValue(QueryKey, out object value) && (query = value as string) != null;
    }

    public static void RemoveQuery(this IQueryNode node)
    {
        node.Data.Remove(QueryKey);
    }
}
