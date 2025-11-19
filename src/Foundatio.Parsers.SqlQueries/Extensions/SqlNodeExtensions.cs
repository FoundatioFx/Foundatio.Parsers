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

        var field = GetFieldInfo(context.Fields, node.Field);
        var (fieldPrefix, fieldSuffix) = field.GetFieldPrefixAndSuffix();

        var builder = new StringBuilder();

        builder.Append(fieldPrefix);
        builder.Append(field.Name);
        if (!node.IsNegated.HasValue || !node.IsNegated.Value)
            builder.Append(" != null");
        else
            builder.Append(" == null");
        builder.Append(fieldSuffix);

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

        var field = GetFieldInfo(context.Fields, node.Field);
        var (fieldPrefix, fieldSuffix) = field.GetFieldPrefixAndSuffix();

        var builder = new StringBuilder();
        builder.Append(fieldPrefix);
        builder.Append(field.Name);
        if (!node.IsNegated.HasValue || !node.IsNegated.Value)
            builder.Append(" == null");
        else
            builder.Append(" != null");
        builder.Append(fieldSuffix);

        return builder.ToString();
    }

    public static string ToDynamicLinqString(this TermNode node, ISqlQueryVisitorContext context)
    {
        if (!String.IsNullOrEmpty(node.Prefix))
            context.AddValidationError("Prefix is not supported for term range queries.");

        // support overriding the generated query
        if (node.TryGetQuery(out string query))
            return query;

        var builder = new StringBuilder();

        if (String.IsNullOrEmpty(node.Field))
        {
            if (context.DefaultFields == null || context.DefaultFields.Length == 0)
            {
                context.AddValidationError("Field or DefaultFields is required for term queries.");
                return String.Empty;
            }

            var fieldTerms = new Dictionary<EntityFieldInfo, SearchTerm>();
            foreach (string df in context.DefaultFields)
            {
                var fieldInfo = GetFieldInfo(context.Fields, df);
                if (!fieldTerms.TryGetValue(fieldInfo, out var searchTerm))
                {
                    searchTerm = new SearchTerm
                    {
                        FieldInfo = fieldInfo,
                        Term = node.Term,
                        Operator = context.DefaultSearchOperator
                    };
                    fieldTerms[fieldInfo] = searchTerm;
                }

                context.SearchTokenizer.Invoke(searchTerm);
                if (searchTerm.Tokens == null)
                    searchTerm.Tokens = [searchTerm.Term];
                else
                    searchTerm.Tokens = searchTerm.Tokens.Select(t => !String.IsNullOrWhiteSpace(t) ? t : "@__NOMATCH__").ToList();
            }

            fieldTerms.Where(f => f.Value.Tokens is { Count: > 0 }).ForEach((kvp, x) =>
            {
                if (x.IsFirst && node.IsNegated.HasValue && node.IsNegated.Value)
                    builder.Append("!");

                builder.Append(x.IsFirst ? "(" : " OR ");

                var searchTerm = kvp.Value;
                var tokens = kvp.Value.Tokens ?? [kvp.Value.Term];
                var (fieldPrefix, fieldSuffix) = kvp.Key.GetFieldPrefixAndSuffix();

                if (searchTerm.Operator == SqlSearchOperator.Equals)
                {
                    builder.Append(fieldPrefix);
                    builder.Append(kvp.Key.Name);
                    builder.Append(" in (");
                    for (int i = 0; i < tokens.Count; i++)
                    {
                        if (i > 0)
                            builder.Append(", ");

                        AppendField(builder, kvp.Key, tokens[i], context);
                    }
                    builder.Append(")");
                    builder.Append(fieldSuffix);
                }
                else if (searchTerm.Operator == SqlSearchOperator.Contains)
                {
                    tokens.ForEach((token, i) =>
                    {
                        builder.Append(i.IsFirst ? "(" : " OR ");
                        builder.Append(fieldPrefix);

                        if (context.FullTextFields.Contains(kvp.Key.Name, StringComparer.OrdinalIgnoreCase))
                        {
                            builder.Append("FTS.Contains(");
                            builder.Append(kvp.Key.Name);
                            builder.Append(", ");
                            AppendField(builder, kvp.Key, token, context);
                            builder.Append(")");
                        }
                        else
                        {
                            builder.Append(kvp.Key.Name);
                            builder.Append(".Contains(");
                            AppendField(builder, kvp.Key, token, context);
                            builder.Append(")");
                        }

                        builder.Append(fieldSuffix);
                        if (i.IsLast)
                            builder.Append(")");
                    });
                }
                else if (searchTerm.Operator == SqlSearchOperator.StartsWith)
                {
                    tokens.ForEach((token, i) =>
                    {
                        builder.Append(i.IsFirst ? "(" : " OR ");
                        builder.Append(fieldPrefix);

                        if (context.FullTextFields.Contains(kvp.Key.Name, StringComparer.OrdinalIgnoreCase))
                        {
                            builder.Append("FTS.Contains(");
                            builder.Append(kvp.Key.Name);
                            builder.Append(", ");
                            AppendField(builder, kvp.Key, "\\\"" + token + "*\\\"", context);
                            builder.Append(")");
                        }
                        else
                        {
                            builder.Append(kvp.Key.Name);
                            builder.Append(".StartsWith(");
                            AppendField(builder, kvp.Key, token, context);
                            builder.Append(")");
                        }

                        builder.Append(fieldSuffix);
                        if (i.IsLast)
                            builder.Append(")");
                    });
                }

                if (x.IsLast)
                    builder.Append(")");
            });

            return builder.ToString();
        }

        var field = GetFieldInfo(context.Fields, node.Field);
        var (fieldPrefix, fieldSuffix) = field.GetFieldPrefixAndSuffix();
        var searchOperator = SqlSearchOperator.Equals;
        if (node.Term.StartsWith("*") && node.Term.EndsWith("*"))
            searchOperator = SqlSearchOperator.Contains;
        else if (node.Term.EndsWith("*"))
            searchOperator = SqlSearchOperator.StartsWith;

        if (node.IsNegated.HasValue && node.IsNegated.Value)
            builder.Append("!");

        if (searchOperator == SqlSearchOperator.Equals)
        {
            builder.Append(fieldPrefix);
            builder.Append(field.Name);
            builder.Append(" = ");
            AppendField(builder, field, node.Term, context);
            builder.Append(fieldSuffix);
        }
        else if (searchOperator == SqlSearchOperator.Contains)
        {
            builder.Append(fieldPrefix);

            if (context.FullTextFields.Contains(field.Name, StringComparer.OrdinalIgnoreCase))
            {
                builder.Append("FTS.Contains(");
                builder.Append(field.Name);
                builder.Append(", ");
                AppendField(builder, field, node.Term, context);
                builder.Append(")");
            }
            else
            {
                builder.Append(field.Name);
                builder.Append(".Contains(");
                AppendField(builder, field, node.Term, context);
                builder.Append(")");
            }

            builder.Append(fieldSuffix);
        }
        else
        {
            builder.Append(fieldPrefix);

            if (context.FullTextFields.Contains(field.Name, StringComparer.OrdinalIgnoreCase))
            {
                builder.Append("FTS.Contains(");
                builder.Append(field.Name);
                builder.Append(", ");
                AppendField(builder, field, "\\\"" + node.Term + "*\\\"", context);
                builder.Append(")");
            }
            else
            {
                builder.Append(field.Name);
                builder.Append(".Contains(");
                AppendField(builder, field, node.Term, context);
                builder.Append(")");
            }

            builder.Append(fieldSuffix);
        }

        return builder.ToString();
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
        if (!field.IsNumber && !field.IsDateOnly && !field.IsDate && !field.IsMoney)
            context.AddValidationError("Field must be a number, money or date for term range queries.");

        var (fieldPrefix, fieldSuffix) = field.GetFieldPrefixAndSuffix();

        var builder = new StringBuilder();

        if (node.IsNegated.HasValue && node.IsNegated.Value)
            builder.Append("!");

        if (node.Min != null && node.Max != null)
            builder.Append("(");

        if (node.Min != null)
        {
            builder.Append(fieldPrefix);
            builder.Append(field.Name);
            builder.Append(node.MinInclusive == true ? " >= " : " > ");
            AppendField(builder, field, node.Min, context);
            builder.Append(fieldSuffix);
        }

        if (node.Min != null && node.Max != null)
            builder.Append(" AND ");

        if (node.Max != null)
        {
            builder.Append(fieldPrefix);
            builder.Append(field.Name);
            builder.Append(node.MaxInclusive == true ? " <= " : " < ");
            AppendField(builder, field, node.Max, context);
            builder.Append(fieldSuffix);
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

    public static EntityFieldInfo GetFieldInfo(List<EntityFieldInfo> fields, string field)
    {
        if (fields == null)
            return new EntityFieldInfo { Name = field, FullName = field };

        return fields.FirstOrDefault(f => f.FullName.Equals(field, StringComparison.OrdinalIgnoreCase)) ??
               new EntityFieldInfo { Name = field, FullName = field };
    }

    private static void AppendField(StringBuilder builder, EntityFieldInfo field, string term, ISqlQueryVisitorContext context)
    {
        if (field == null)
            return;

        if (field.IsNumber || field.IsBoolean || field.IsMoney)
        {
            builder.Append(term);
        }
        else if (field is { IsDate: true })
        {
            term = term.Trim();

            if (term.StartsWith("now", StringComparison.OrdinalIgnoreCase))
            {
                builder.Append(context.DateTimeParser != null ? context.DateTimeParser("now") : "DateTime.UtcNow");

                if (term.Length == 3)
                    return;

                builder.Append(".");

                string method = term[^1..] switch
                {
                    "y" => "AddYears",
                    "M" => "AddMonths",
                    "d" => "AddDays",
                    "h" => "AddHours",
                    "H" => "AddHours",
                    "m" => "AddMinutes",
                    "s" => "AddSeconds",
                    _ => throw new NotSupportedException("Invalid date operation.")
                };

                bool subtract = term.Substring(3, 1) == "-";

                builder.Append(method).Append("(").Append(subtract ? "-" : "").Append(term.Substring(4, term.Length - 5)).Append(")");
            }
            else
            {
                if (context.DateTimeParser != null)
                {
                    term = context.DateTimeParser(term);
                    builder.Append(term);
                }
                else
                {
                    builder.Append("DateTime.Parse(\"" + term + "\")");
                }
            }
        }
        else if (field is { IsDateOnly: true })
        {
            term = term.Trim();

            if (term.StartsWith("now", StringComparison.OrdinalIgnoreCase))
            {
                builder.Append(context.DateOnlyParser != null
                    ? context.DateOnlyParser("now")
                    : "DateOnly.FromDateTime(DateTime.UtcNow)");

                if (term.Length == 3)
                    return;

                builder.Append(".");

                string method = term[^1..] switch
                {
                    "y" => "AddYears",
                    "M" => "AddMonths",
                    "d" => "AddDays",
                    _ => throw new NotSupportedException("Invalid date operation.")
                };

                bool subtract = term.Substring(3, 1) == "-";

                builder.Append(method).Append("(").Append(subtract ? "-" : "").Append(term.Substring(4, term.Length - 5)).Append(")");
            }
            else
            {
                if (context.DateOnlyParser != null)
                {
                    term = context.DateOnlyParser(term);
                    builder.Append(term);
                }
                else
                {
                    builder.Append("DateOnly.Parse(\"" + term + "\")");
                }
            }
        }
        else
            builder.Append("\"" + term + "\"");
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
