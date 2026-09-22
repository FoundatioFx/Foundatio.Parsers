import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;

import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.core.KeywordAnalyzer;
import org.apache.lucene.analysis.miscellaneous.PerFieldAnalyzerWrapper;
import org.apache.lucene.analysis.standard.StandardAnalyzer;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.StringField;
import org.apache.lucene.document.TextField;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.queryparser.classic.ParseException;
import org.apache.lucene.queryparser.classic.QueryParser;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.Query;
import org.apache.lucene.store.ByteBuffersDirectory;
import org.apache.lucene.util.Version;

// Run with the jars and JDK from the tested Elasticsearch distribution, not a second Lucene dependency.
public class LuceneCompatibility {
    public static void main(String[] args) throws Exception {
        if (args.length != 1)
            throw new IllegalArgumentException("Expected the compatibility corpus directory");
        Path root = Path.of(args[0]);
        List<String> failures = new ArrayList<>();
        int verified = 0;
        int mappingSpecific = 0;

        try (Analyzer standard = new StandardAnalyzer();
             Analyzer keyword = new KeywordAnalyzer();
             Analyzer analyzer = new PerFieldAnalyzerWrapper(standard, Map.of("keyword", keyword));
             var directory = new ByteBuffersDirectory();
             var writer = new IndexWriter(directory, new IndexWriterConfig(analyzer))) {
            var documentIds = new HashSet<String>();
            for (String[] values : rows(root.resolve("documents.tsv"), 6)) {
                String id = values[0].trim();
                if (id.isEmpty() || !documentIds.add(id))
                    throw new IllegalArgumentException("Invalid or duplicate document ID: " + id);
                Document document = new Document();
                document.add(new StringField("id", id, Field.Store.YES));
                if (!values[1].equals("-"))
                    document.add(new TextField("text", values[1], Field.Store.NO));
                if (!values[2].equals("-"))
                    document.add(new TextField("otherText", values[2], Field.Store.NO));
                if (!values[3].equals("-"))
                    document.add(new StringField("keyword", values[3], Field.Store.NO));
                writer.addDocument(document);
            }
            if (documentIds.size() != 12)
                throw new AssertionError("Expected 12 corpus documents");
            writer.commit();

            try (var reader = DirectoryReader.open(writer)) {
                var searcher = new IndexSearcher(reader);
                var caseIds = new HashSet<String>();
                System.out.println("Lucene " + Version.LATEST + "; documents=" + reader.numDocs());
                System.out.println("id\tquery\texpected\tactual\tscores");
                for (String[] values : rows(root.resolve("cases.tsv"), 7)) {
                    if (!caseIds.add(values[0]))
                        throw new IllegalArgumentException("Duplicate case: " + values[0]);
                    if (values[5].equals("N/A")) {
                        mappingSpecific++;
                        System.out.println(values[0] + "\tMAPPING_SPECIFIC\t" + values[6]);
                        continue;
                    }
                    var parser = parser(analyzer, values[2]);
                    String actual;
                    Map<String, Float> scores = Map.of();
                    try {
                        scores = scores(searcher, parser.parse(values[1]));
                        actual = scores.isEmpty() ? "-" : String.join(",", scores.keySet());
                    } catch (ParseException exception) {
                        actual = "ERROR";
                    }
                    verified++;
                    System.out.println(values[0] + "\t" + values[1] + "\t" + values[5] + "\t" + actual + "\t" + scores);
                    if (!actual.equals(values[5]))
                        failures.add(values[0] + ": expected=" + values[5] + ", actual=" + actual);
                }

                var parser = parser(analyzer, "OR");
                for (String text : List.of("text:alpha", "text:\"alpha beta\"", "(text:alpha OR text:gamma)")) {
                    var baseline = scores(searcher, parser.parse(text));
                    var boosted = scores(searcher, parser.parse(text + "^8"));
                    if (baseline.isEmpty() || !baseline.keySet().equals(boosted.keySet()))
                        failures.add("Invalid score control: " + text);
                    for (var hit : baseline.entrySet()) {
                        double expected = hit.getValue() * 8;
                        if (Math.abs(boosted.get(hit.getKey()) - expected) > 0.00001 * Math.max(1, Math.abs(expected)))
                            failures.add("Boost multiplier: " + text + " / " + hit.getKey());
                    }
                }
                var ranking = scores(searcher, parser.parse("text:alpha^8 OR text:gamma"));
                if (!(ranking.get("a") > ranking.get("c")))
                    failures.add("Boost failed to reverse the a/c ranking");
                System.out.println("Verified " + verified + " matching cases and 4 scoring controls; mapping-specific=" + mappingSpecific);
            }
        }
        if (!failures.isEmpty())
            throw new AssertionError(String.join("\n", failures));
    }

    private static QueryParser parser(Analyzer analyzer, String operator) {
        var parser = new QueryParser("text", analyzer);
        parser.setDefaultOperator(operator.equals("AND") ? QueryParser.Operator.AND : QueryParser.Operator.OR);
        parser.setAllowLeadingWildcard(true);
        return parser;
    }

    private static Map<String, Float> scores(IndexSearcher searcher, Query query) throws Exception {
        var hits = searcher.search(query, 100);
        if (searcher.count(query) != hits.scoreDocs.length)
            throw new AssertionError("Truncated search results");
        Map<String, Float> scores = new TreeMap<>();
        for (var hit : hits.scoreDocs) {
            String id = searcher.storedFields().document(hit.doc).get("id");
            if (!Float.isFinite(hit.score) || scores.put(id, hit.score) != null)
                throw new AssertionError("Invalid score or duplicate document: " + id);
        }
        return scores;
    }

    private static List<String[]> rows(Path path, int columns) throws Exception {
        List<String[]> result = new ArrayList<>();
        for (String line : Files.readAllLines(path)) {
            if (line.isBlank() || line.startsWith("#"))
                continue;
            String[] values = line.split("\t", -1);
            if (values.length != columns)
                throw new IllegalArgumentException("Invalid row in " + path + ": " + line);
            result.add(values);
        }
        if (result.isEmpty())
            throw new IllegalArgumentException("Empty corpus: " + path);
        return result;
    }
}
