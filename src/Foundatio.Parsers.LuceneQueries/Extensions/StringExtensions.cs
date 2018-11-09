using System;
using System.Linq;
using System.Text;

namespace Foundatio.Parsers.LuceneQueries.Extensions {
    public static class StringExtensions {
        public static string Unescape(this string input) {
            if (input == null)
                return null;

            var sb = new StringBuilder();
            var escaped = false;
            foreach (var ch in input) {
                if (escaped) {
                    sb.Append(ch);
                    escaped = false;
                } else if (ch == '\\') {
                    escaped = true;
                } else {
                    sb.Append(ch);
                }
            }

            if (escaped)
                sb.Append('\\');

            return sb.ToString();
        }

        //private static readonly char[] LuceneSpecialCharacters = { '+', '-', '&', '|', '!', '(', ')', '{', '}', '[', ']', '^', '"', '~', '*', '?', ':', '\\' };
        private static readonly char[] LuceneSpecialCharacters = { ':', '\\' };

        public static string Escape(this string input) {
            if (input == null)
                return null;

            var sb = new StringBuilder();
            foreach (var ch in input) {
                if (LuceneSpecialCharacters.Contains(ch))
                    sb.Append("\\" + ch);
                else
                    sb.Append(ch);
            }

            return sb.ToString();
        }
    }
}
