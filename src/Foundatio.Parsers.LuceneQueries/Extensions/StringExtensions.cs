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

        // Lucene:  + - && || ! ( ) { } [ ] ^ " ~ * ? : \
        // Elastic: + - && || ! ( ) { } [ ] ^ " ~ * ? : \ / = > <
        //private static readonly char[] LuceneSpecialCharacters = { '+', '-', '&', '|', '!', '(', ')', '{', '}', '[', ']', '^', '"', '~', '*', '?', ':', '\\' };
        private static readonly char[] LuceneSpecialCharacters = { ':', '\\' };

        // NOTE: This is not being used as the parser itself handles escape sequences and queries need to be escaped (not auto escaped)
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
