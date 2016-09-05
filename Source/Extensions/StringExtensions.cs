using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Exceptionless.LuceneQueryParser.Extensions
{
    internal static class StringExtensions
    {
        public static string Unescape(this string input) {
            if (input == null)
                return null;

            var sb = new StringBuilder();
            var escaped = false;
            foreach (var ch in input)
            {
                if (escaped)
                {
                    sb.Append(ch);
                    escaped = false;
                }
                else if (ch == '\\')
                {
                    escaped = true;
                }
                else
                {
                    sb.Append(ch);
                }
            }
            if (escaped)
                sb.Append('\\');
            return sb.ToString();
        }

        private static readonly char[] LuceneSpecialCharacters = new[] {'+', '-', '&', '|', '!', '(', ')', '{', '}', '[', ']', '^', '"', '~', '*', '?', ':', '\\'};

        public static string Escape(this string input) {
            if (input == null)
                return null;

            var sb = new StringBuilder();
            foreach (var ch in input)
            {
                if (LuceneSpecialCharacters.Contains(ch))
                    sb.Append("\\" + ch);
                else
                    sb.Append(ch);
            }
            return sb.ToString();
        }
    }
}
