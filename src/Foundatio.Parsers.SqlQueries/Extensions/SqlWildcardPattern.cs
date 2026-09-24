using System;
using System.Collections.Generic;
using System.Text;
using Foundatio.Parsers.LuceneQueries.Nodes;

namespace Foundatio.Parsers.SqlQueries.Extensions;

internal enum SqlWildcardKind { None, Prefix, Contains, Advanced }

internal sealed class SqlWildcardPattern
{
    private SqlWildcardPattern(SqlWildcardKind kind, string literal, string likePattern)
    {
        Kind = kind;
        Literal = literal;
        LikePattern = likePattern;
    }

    public SqlWildcardKind Kind { get; }
    public string Literal { get; }
    public string LikePattern { get; }

    public static SqlWildcardPattern Analyze(TermNode node)
    {
        string term = node.Term ?? String.Empty;
        if (node.IsQuotedTerm || node.IsRegexTerm)
            return new SqlWildcardPattern(SqlWildcardKind.None, node.UnescapedTerm ?? String.Empty, String.Empty);

        var literal = new StringBuilder();
        var pattern = new StringBuilder();
        var wildcards = new List<(char Value, int Position)>();
        bool escaped = false;
        foreach (char c in term)
        {
            if (!escaped && c == '\\')
            {
                escaped = true;
                continue;
            }

            if (!escaped && (c == '*' || c == '?'))
            {
                wildcards.Add((c, literal.Length));
                pattern.Append(c == '*' ? '%' : '_');
            }
            else
            {
                literal.Append(c);
                if (c is '%' or '_' or '[' or ']' or '\\')
                    pattern.Append('\\');
                pattern.Append(c);
            }

            escaped = false;
        }

        if (escaped)
        {
            literal.Append('\\');
            pattern.Append("\\\\");
        }

        var kind = SqlWildcardKind.None;
        if (wildcards.Count == 1 && wildcards[0] == ('*', literal.Length))
            kind = SqlWildcardKind.Prefix;
        else if (wildcards.Count == 2 && wildcards[0] == ('*', 0) && wildcards[1] == ('*', literal.Length))
            kind = SqlWildcardKind.Contains;
        else if (wildcards.Count > 0)
            kind = SqlWildcardKind.Advanced;

        return new SqlWildcardPattern(kind, literal.ToString(), pattern.ToString());
    }
}
