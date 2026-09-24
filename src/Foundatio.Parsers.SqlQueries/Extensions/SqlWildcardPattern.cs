using System;
using System.Text;
using Foundatio.Parsers.LuceneQueries.Nodes;

namespace Foundatio.Parsers.SqlQueries.Extensions;

internal enum SqlWildcardKind { None, Prefix, Contains, Advanced }

internal static class SqlWildcardPattern
{
    public static SqlWildcardKind GetKind(TermNode node)
    {
        if (node.IsQuotedTerm || node.IsRegexTerm || node.Term is not { Length: > 0 } term)
            return SqlWildcardKind.None;

        int literalLength = 0;
        int wildcardCount = 0;
        int firstPosition = -1;
        int secondPosition = -1;
        char firstWildcard = default;
        char secondWildcard = default;
        bool escaped = false;

        foreach (char character in term)
        {
            if (!escaped && character is '\\')
            {
                escaped = true;
                continue;
            }

            if (!escaped && character is '*' or '?')
            {
                wildcardCount++;
                if (wildcardCount is 1)
                {
                    firstPosition = literalLength;
                    firstWildcard = character;
                }
                else if (wildcardCount is 2)
                {
                    secondPosition = literalLength;
                    secondWildcard = character;
                }
            }
            else
                literalLength++;

            escaped = false;
        }

        if (wildcardCount is 1 && firstWildcard is '*' && firstPosition == literalLength)
            return SqlWildcardKind.Prefix;
        if (wildcardCount is 2 && firstWildcard is '*' && firstPosition is 0
            && secondWildcard is '*' && secondPosition == literalLength)
            return SqlWildcardKind.Contains;
        return wildcardCount is 0 ? SqlWildcardKind.None : SqlWildcardKind.Advanced;
    }

    public static string GetLiteral(TermNode node, SqlWildcardKind kind)
    {
        string literal = node.Term is { } term && term.IndexOf('\\') < 0
            ? term
            : node.UnescapedTerm ?? String.Empty;
        return kind switch
        {
            SqlWildcardKind.Prefix => literal[..^1],
            SqlWildcardKind.Contains => literal[1..^1],
            _ => literal
        };
    }

    public static string GetLikePattern(TermNode node)
    {
        var pattern = new StringBuilder(node.Term?.Length ?? 0);
        bool escaped = false;
        foreach (char character in node.Term ?? String.Empty)
        {
            if (!escaped && character is '\\')
            {
                escaped = true;
                continue;
            }

            if (!escaped && character is '*' or '?')
                pattern.Append(character is '*' ? '%' : '_');
            else
            {
                if (character is '%' or '_' or '[' or ']' or '\\')
                    pattern.Append('\\');
                pattern.Append(character);
            }

            escaped = false;
        }

        if (escaped)
            pattern.Append("\\\\");

        return pattern.ToString();
    }
}
