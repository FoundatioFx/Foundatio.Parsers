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
    }
}
