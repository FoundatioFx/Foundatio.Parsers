using System;
using System.Threading.Tasks;
using Foundatio.Xunit;
using Foundatio.Parsers.LuceneQueries;
using Foundatio.Parsers.LuceneQueries.Nodes;
using Foundatio.Parsers.LuceneQueries.Visitors;
using Microsoft.Extensions.Logging;
using Xunit;
using Xunit.Abstractions;
using Pegasus.Common.Tracing;
using Pegasus.Common;
using Microsoft.Extensions.Logging.Abstractions;
using System.Text;
using System.Collections.Generic;

namespace Foundatio.Parsers.Tests {
    public class GenerateQueryVisitorTests : TestWithLoggingBase {
        public GenerateQueryVisitorTests(ITestOutputHelper output) : base(output) {
            Log.MinimumLevel = Microsoft.Extensions.Logging.LogLevel.Trace;
        }

        [Theory]
        [InlineData(null, null, false)]
        [InlineData(":", null, false)]
        [InlineData("\":\"", "\":\"", true)]
        [InlineData("  \t", "", true)]
        [InlineData("string\"", "string\"", true)]
        [InlineData("\"quoted string\"", "\"quoted string\"", true)]
        [InlineData("criteria", "criteria", true)]
        [InlineData("(criteria)", "(criteria)", true)]
        [InlineData("field:criteria", "field:criteria", true)]
        [InlineData("field :criteria", "field:criteria", true)]
        [InlineData("-criteria", "-criteria", true)]
        [InlineData("+criteria", "+criteria", true)]
        [InlineData("criteria1 AND NOT criteria2", "criteria1 AND NOT criteria2", true)]
        [InlineData("criteria1 NOT criteria2", "criteria1 NOT criteria2", true)]
        [InlineData("field:criteria1 NOT field:criteria2", "field:criteria1 NOT field:criteria2", true)]
        [InlineData("criteria1   criteria2", "criteria1 criteria2", true)]
        [InlineData("criteria1 +criteria2", "criteria1 +criteria2", true)]
        [InlineData("criteria1 OR criteria2", "criteria1 OR criteria2", true)]
        [InlineData("criteria1 OR criteria2 OR criteria3", "criteria1 OR criteria2 OR criteria3", true)]
        [InlineData("criteria1 OR (criteria2 AND criteria3)", "criteria1 OR (criteria2 AND criteria3)", true)]
        [InlineData("field:[1 TO 2]", "field:[1 TO 2]", true)]
        [InlineData("field:{1 TO 2}", "field:{1 TO 2}", true)]
        [InlineData("field:[1 TO 2}", "field:[1 TO 2}", true)]
        [InlineData("field:(criteria1 criteria2)", "field:(criteria1 criteria2)", true)]
        [InlineData("data.field:(now criteria2)", "data.field:(now criteria2)", true)]
        [InlineData("field:(criteria1 OR criteria2)", "field:(criteria1 OR criteria2)", true)]
        [InlineData("field:*cr", "field:*cr", true)] // TODO lucene doesn't support wildcards at the beginning.
        [InlineData("field:c*r", "field:c*r", true)]
        [InlineData("field:cr*", "field:cr*", true)]
        [InlineData("field:*", "field:*", false)]
        [InlineData("date:>now", "date:>now", true)]
        [InlineData("date:<now", "date:<now", true)]
        [InlineData("_exists_:title", "_exists_:title", true)]
        [InlineData("_missing_:title", "_missing_:title", true)]
        [InlineData("book.\\*:(quick brown)", "book.\\*:(quick brown)", true)]
        [InlineData("date:[now/d-4d TO now/d+1d}", @"date:[now/d-4d TO now/d+1d}", true)]
        [InlineData("(date:[now/d-4d TO now/d+1d})", @"(date:[now/d-4d TO now/d+1d})", true)]
        [InlineData("data.date:>now", "data.date:>now", true)]
        [InlineData("data.date:[now/d-4d TO now/d+1d}", @"data.date:[now/d-4d TO now/d+1d}", true)]
        [InlineData("data.date:[2012-01-01 TO 2012-12-31]", "data.date:[2012-01-01 TO 2012-12-31]", true)]
        [InlineData("data.date:[* TO 2012-12-31]", "data.date:[* TO 2012-12-31]", true)]
        [InlineData("data.date:[2012-01-01 TO *]", "data.date:[2012-01-01 TO *]", true)]
        [InlineData("(data.date:[now/d-4d TO now/d+1d})", @"(data.date:[now/d-4d TO now/d+1d})", true)]
        [InlineData("criter~", "criter~", true)]
        [InlineData("criter~1", "criter~1", true)]
        [InlineData("roam~0.8", "roam~0.8", true)]
        [InlineData(@"date^""America/Chicago_Other""", @"date^America/Chicago_Other", true)]
        [InlineData("criter^2", "criter^2", true)]
        [InlineData("\"blah criter\"~1", "\"blah criter\"~1", true)]
        [InlineData("count:>1", "count:>1", true)]
        [InlineData(@"book.\*:test", "book.\\*:test", true)]
        [InlineData("count:>=1", "count:>=1", true)]
        [InlineData("count:[1..5}", "count:[1..5}", true)]
        [InlineData(@"count:a\:a", @"count:a\:a", true)]
        [InlineData("count:a:a", null, false)]
        [InlineData(@"count:a\:a more:stuff", @"count:a\:a more:stuff", true)]
        [InlineData("data.count:[1..5}", "data.count:[1..5}", true)]
        [InlineData("age:(>=10 AND < 20)", "age:(>=10 AND <20)", true)]
        [InlineData("age : >= 10", "age:>=10", true)]
        [InlineData("age:[1 TO 2]", "age:[1 TO 2]", true)]
        [InlineData("data.Windows-identity:ejsmith", "data.Windows-identity:ejsmith", true)]
        [InlineData("data.age:(>30 AND <=40)", "data.age:(>30 AND <=40)", true)]
        [InlineData("+>=10", "+>=10", true)]
        [InlineData(">=10", ">=10", true)]
        [InlineData("age:(+>=10)", "age:(+>=10)", true)]
        [InlineData("data.age:(+>=10 AND < 20)", "data.age:(+>=10 AND <20)", true)]
        [InlineData("data.age:(+>=10 +<20)", "data.age:(+>=10 +<20)", true)]
        [InlineData("data.age:(->=10 AND < 20)", "data.age:(->=10 AND <20)", true)]
        [InlineData("data.age:[10 TO *]", "data.age:[10 TO *]", true)]
        [InlineData("title:(full text search)^2", "title:(full text search)^2", true)]
        [InlineData("data.age:[* TO 10]", "data.age:[* TO 10]", true)]
        [InlineData("data:[* TO 10]^hey", "data:[* TO 10]^hey", true)]
        [InlineData("hidden:true AND data.age:(>30 AND <=40)", "hidden:true AND data.age:(>30 AND <=40)", true)]
        [InlineData("hidden:true", "hidden:true", true)]
        [InlineData("geo:\"Dallas, TX\"~75m", "geo:\"Dallas, TX\"~75m", true)]
        [InlineData("geo:\"Dallas, TX\"~75 m", "geo:\"Dallas, TX\"~75 m", true)]
        [InlineData("min:price geogrid:geo~6 count:(category count:subcategory avg:price min:price)", "min:price geogrid:geo~6 count:(category count:subcategory avg:price min:price)", true)]
        [InlineData("datehistogram:(date~2^-5\\:30 min:date)", "datehistogram:(date~2^-5\\:30 min:date)", true)]
        [InlineData("-type:404", "-type:404", true)]
        [InlineData("type:test?s", "type:test?s", true)]
        [InlineData("NOT Test", "NOT Test", true)]
        [InlineData("! Test", "! Test", true)] // The symbol ! can be used in place of the word NOT.
        [InlineData("type:?", "type:?", false)]
        // TODO: Need to work on allowing more characters to be escaped
        //[InlineData(@"type:\(11\)2\+", @"type:\(11\)2\+", true)]
        [InlineData(@"""\""now""", @"""\""now""", true)]
        [InlineData("title:(+return +\"pink panther\")", "title:(+return +\"pink panther\")", true)]
        [InlineData("\"jakarta apache\" -\"Apache Lucene\"", "\"jakarta apache\" -\"Apache Lucene\"", true)]
        [InlineData("\"jakarta apache\"^4 \"Apache Lucene\"", "\"jakarta apache\"^4 \"Apache Lucene\"", true)]
        [InlineData("NOT \"jakarta apache\"", "NOT \"jakarta apache\"", true)]
        [InlineData(@"updated:2016-09-02T15\:41\:43.3385286Z", @"updated:2016-09-02T15\:41\:43.3385286Z", true)]
        [InlineData(@"updated:>2016-09-02T15\:41\:43.3385286Z", @"updated:>2016-09-02T15\:41\:43.3385286Z", true)]
        [InlineData(@"field1:""\""value1\""""", @"field1:""\""value1\""""", true)]
        [InlineData(@"""\""value1""", @"""\""value1""", true)]
        [InlineData(@"( ( cat AND dog ))", @"((cat AND dog))", true)]
        public async Task CanGenerateQueryAsync(string query, string expected, bool isValid) {
            var parser = new LuceneQueryParser();
            Log.MinimumLevel = LogLevel.Information;

            IQueryNode result;
            try {
                result = await parser.ParseAsync(query);
            } catch (Exception ex) {
                Assert.False(isValid, ex.Message);
                return;
            }

            string nodes = await DebugQueryVisitor.RunAsync(result);
            _logger.LogInformation(nodes);
            string generatedQuery = await GenerateQueryVisitor.RunAsync(result);
            Assert.Equal(expected, generatedQuery);
        }

        [Fact]
        public async Task CanGenerateSingleQueryAsync() {
            string query = "datehistogram:(date~2^-5\\:30 min:date max:date)";
            string expected = "datehistogram:(date~2^-5\\:30 min:date max:date)";
            var parser = new LuceneQueryParser();

            var result = await parser.ParseAsync(query);

            _logger.LogInformation(await DebugQueryVisitor.RunAsync(result));
            string generatedQuery = await GenerateQueryVisitor.RunAsync(result);
            Assert.Equal(expected, generatedQuery);

            await new AssignOperationTypeVisitor().AcceptAsync(result, null);
            _logger.LogInformation(await DebugQueryVisitor.RunAsync(result));
        }

        [Theory]
        [InlineData("+")]
        [InlineData("-")]
        [InlineData("!")]
        [InlineData("(")]
        [InlineData(")")]
        [InlineData("{")]
        [InlineData("}")]
        [InlineData("[")]
        [InlineData("]")]
        [InlineData("^")]
        [InlineData("\"")]
        [InlineData("~")]
        [InlineData("*")]
        [InlineData("?")]
        [InlineData(":")]
        [InlineData("\\")]
        public async Task CanParseEscapedQuery(string escaped) {
            // https://lucene.apache.org/core/2_9_4/queryparsersyntax.html#Escaping%20Special%20Characters
            // + - && || ! ( ) { } [ ] ^ " ~ * ? : \
            string query = @"\" + escaped;
            var tracer = new StringBuilderTrace();
            var parser = new LuceneQueryParser {
                //Tracer = tracer
            };

            try {
                _logger.LogInformation($"Attempting: {escaped}");
                var result = await parser.ParseAsync(query);

                _logger.LogInformation(await DebugQueryVisitor.RunAsync(result));
                string generatedQuery = await GenerateQueryVisitor.RunAsync(result);
                Assert.Equal(query, generatedQuery);
            } catch (FormatException ex) {
                _logger.LogInformation(tracer.ToString());
                var cursor = ex.Data["cursor"] as Cursor;
                throw new FormatException($"[{cursor.Line}:{cursor.Column}] {ex.Message}", ex);
            }
        }

        [Fact]
        public async Task CanUseForwardSlashes() {
            string query = @"hey/now";
            var tracer = new StringBuilderTrace();
            var parser = new LuceneQueryParser {
                //Tracer = tracer
            };

            try {
                var result = await parser.ParseAsync(query);

                _logger.LogInformation(await DebugQueryVisitor.RunAsync(result));
                string generatedQuery = await GenerateQueryVisitor.RunAsync(result);
                Assert.Equal(query, generatedQuery);
            } catch (FormatException ex) {
                _logger.LogInformation(tracer.ToString());
                var cursor = ex.Data["cursor"] as Cursor;
                throw new FormatException($"[{cursor.Line}:{cursor.Column}] {ex.Message}", ex);
            }
        }

        [Fact]
        public void CanParseQueryConcurrently() {
            var parser = new LuceneQueryParser();
            Parallel.For(0, 100, i => {
                var result = parser.Parse("criteria   some:criteria blah:(more      stuff)");
                Assert.NotNull(result);
            });
        }
    }

    public class StringBuilderTrace : ITracer {
        private readonly StringBuilder _builder = new StringBuilder();
        private int _indentLevel = 0;

        public override string ToString() => _builder.ToString();

        public void TraceCacheHit<T>(string ruleName, Cursor cursor, CacheKey cacheKey, IParseResult<T> parseResult) => TraceInfo(ruleName, cursor, "Cache hit.");

        public void TraceCacheMiss(string ruleName, Cursor cursor, CacheKey cacheKey) => TraceInfo(ruleName, cursor, "Cache miss.");

        public void TraceInfo(string ruleName, Cursor cursor, string info) => _builder.Append(new string(' ', _indentLevel)).AppendLine(info);

        public void TraceRuleEnter(string ruleName, Cursor cursor) {
            _builder.AppendLine(new string(' ', _indentLevel)).Append($"Begin '{ruleName}' at ({cursor.Line},{cursor.Column}) with state key {cursor.StateKey}");
            _indentLevel++;
        }

        public void TraceRuleExit<T>(string ruleName, Cursor cursor, IParseResult<T> parseResult) {
            var success = parseResult != null;
            _indentLevel--;
            _builder.AppendLine(new string(' ', _indentLevel)).Append($"End '{ruleName}' with {(success ? "success" : "failure")} at ({cursor.Line},{cursor.Column}) with state key {cursor.StateKey}");
        }
    }
}
