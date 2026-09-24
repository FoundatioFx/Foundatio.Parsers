using System;
using System.Globalization;
using System.Linq;
using System.Text;
using Elastic.Clients.Elasticsearch;
using Elastic.Clients.Elasticsearch.QueryDsl;
using Foundatio.Parsers.ElasticQueries.Visitors;
using Foundatio.Parsers.LuceneQueries.Extensions;
using Foundatio.Parsers.LuceneQueries.Nodes;
using Foundatio.Parsers.LuceneQueries.Visitors;

namespace Foundatio.Parsers.ElasticQueries.Extensions;

/// <summary>
/// Reads term modifiers once while retaining raw escape intent for mapping-specific query generation.
/// Expressions contain only an escaped term or delimited regex, never a complete user query.
/// </summary>
internal readonly record struct QueryTerm
{
    public required string Value { get; init; }
    public bool Quoted { get; init; }
    public string? Regex { get; init; }
    public string? Wildcard { get; init; }
    public string? Expression { get; init; }
    public bool IsPrefix { get; init; }
    public float? Boost { get; init; }
    public int? Slop { get; init; }
    public Fuzziness? Fuzziness { get; init; }

    public static bool TryCreate(TermNode node, IElasticQueryVisitorContext context, out QueryTerm term)
    {
        term = default;
        if (!TryReadBoost(node.UnescapedBoost, context, out var boost))
            return false;

        string raw = node.Term!;
        string? wildcard = null;
        string? expression = null;
        bool prefix = false;
        if (!node.IsQuotedTerm && !node.IsRegexTerm && HasWildcard(raw))
        {
            BuildWildcard(raw, out wildcard, out expression, out prefix);
            if (raw[0] is '*' or '?' && !context.GetValidationOptions().AllowLeadingWildcards)
            {
                // The visitor normally reports this first; direct query helpers must also reject it.
                string message = $"Terms must not start with a wildcard: {raw}";
                if (!context.GetValidationErrors().Any(error => error.Index is -1 && String.Equals(error.Message, message, StringComparison.Ordinal)))
                    context.AddValidationError(message);

                return false;
            }
        }

        int? slop = null;
        Fuzziness? fuzziness = null;
        if (node.Proximity is not null)
        {
            if (node.IsRegexTerm || wildcard is not null)
            {
                context.AddValidationError($"Fuzziness cannot be combined with regex or wildcard syntax: {node}");
                return false;
            }

            string proximity = node.UnescapedProximity!;
            if (node.IsQuotedTerm)
            {
                if (proximity.Length is 0)
                    slop = 0;
                else if (Int32.TryParse(proximity, NumberStyles.AllowLeadingSign, CultureInfo.InvariantCulture, out int value) && value is >= 0)
                    slop = value;
                else
                {
                    context.AddValidationError($"Phrase slop must be a non-negative integer: {proximity}");
                    return false;
                }
            }
            else if (proximity.Length is 0)
            {
                fuzziness = new Fuzziness(2);
            }
            else if (Int32.TryParse(proximity, NumberStyles.AllowLeadingSign, CultureInfo.InvariantCulture, out int distance) && distance is >= 0 and <= 2)
            {
                fuzziness = new Fuzziness(distance);
            }
            else
            {
                context.AddValidationError($"Fuzzy edit distance must be 0, 1, or 2: {proximity}");
                return false;
            }
        }

        term = new QueryTerm
        {
            Value = node.UnescapedTerm!,
            Quoted = node.IsQuotedTerm,
            Regex = node.IsRegexTerm ? raw : null,
            Wildcard = wildcard,
            Expression = node.IsRegexTerm ? $"/{raw}/" : expression,
            IsPrefix = prefix,
            Boost = boost,
            Slop = slop,
            Fuzziness = fuzziness
        };
        return true;
    }

    public static bool TryReadBoost(string? value, IQueryVisitorContext context, out float? boost)
    {
        boost = null;
        if (value is null)
            return true;

        if (Single.TryParse(value, NumberStyles.Float, CultureInfo.InvariantCulture, out float parsed) && Single.IsFinite(parsed) && parsed >= 0)
        {
            boost = parsed;
            return true;
        }

        context.AddValidationError($"A query boost must be a finite, non-negative number: {value}");
        return false;
    }

    public static Query? ApplyGroupBoost(Query? query, GroupNode node, IQueryVisitorContext context)
    {
        if (node.Proximity is not null)
        {
            context.AddValidationError($"Group proximity is not supported: {node}");
            return new MatchNoneQuery();
        }

        // Boost metadata retains grammar escapes: ^\+2 must parse as +2, just like term/range boosts.
        if (!TryReadBoost(node.UnescapedBoost, context, out var boost))
            return new MatchNoneQuery();

        // Wrap the completed group once: changing a child boost would lose its existing
        // boost or miss nested/required branches. A bool must wrapper preserves membership.
        return query is null || boost is null ? query : new BoolQuery { Must = [query], Boost = boost };
    }

    public QueryStringQuery ToQueryString(string[]? fields, IElasticQueryVisitorContext context)
    {
        var query = new QueryStringQuery(Expression!)
        {
            AllowLeadingWildcard = context.GetValidationOptions().AllowLeadingWildcards,
            AnalyzeWildcard = true,
            Boost = Boost
        };
        if (fields is { Length: > 0 })
            query.Fields = fields;
        return query;
    }

    private static bool HasWildcard(string raw)
    {
        for (int i = 0; i < raw.Length; i++)
        {
            if (raw[i] is '\\')
                i++;
            else if (raw[i] is '*' or '?')
                return true;
        }
        return false;
    }

    private static void BuildWildcard(string raw, out string wildcard, out string expression, out bool prefix)
    {
        var pattern = new StringBuilder(raw.Length);
        var escaped = new StringBuilder(raw.Length);
        int operators = 0;
        int lastOperator = -1;
        for (int i = 0; i < raw.Length; i++)
        {
            char value = raw[i];
            bool literal = value is '\\';
            if (literal && i + 1 < raw.Length)
                value = raw[++i];

            if (!literal && value is '*' or '?')
            {
                operators++;
                lastOperator = i;
                pattern.Append(value);
                escaped.Append(value);
                continue;
            }

            if (value is '*' or '?' or '\\')
                pattern.Append('\\');
            pattern.Append(value);

            // Parse only this escaped term, never an unescaped field/operator expression.
            if (Char.IsWhiteSpace(value) || "+-=!(){}[]^\"~*?:\\/|&<>".Contains(value))
                escaped.Append('\\');
            escaped.Append(value);
        }
        wildcard = pattern.ToString();
        expression = escaped.ToString();
        prefix = operators is 1 && lastOperator == raw.Length - 1 && raw[^1] is '*';
    }
}
