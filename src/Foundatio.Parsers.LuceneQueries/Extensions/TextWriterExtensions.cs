using System.IO;

namespace Foundatio.Parsers.LuceneQueries.Extensions;

public static class TextWriterExtensions {
    public static void WriteIf(this TextWriter writer, bool condition, string content, params object[] arguments) {
        if (!condition)
            return;

        if (arguments != null && arguments.Length > 0)
            writer.Write(content, arguments);
        else
            writer.Write(content);
    }

    public static void WriteLineIf(this TextWriter writer, bool condition, string content, params object[] arguments) {
        if (!condition)
            return;

        if (arguments != null && arguments.Length > 0)
            writer.WriteLine(content, arguments);
        else
            writer.WriteLine(content);
    }
}
