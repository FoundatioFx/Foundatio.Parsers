#define ENABLE_TRACING

using System;
using System.Threading.Tasks;
using Foundatio.Parsers.LuceneQueries.Nodes;
using Foundatio.Parsers.LuceneQueries.Visitors;
using Foundatio.Xunit;
using Microsoft.Extensions.Logging;
using Pegasus.Common;
using Xunit;
using Xunit.Abstractions;

namespace Foundatio.Parsers.LuceneQueries.Tests;

public class QueryParserValidationTests : TestWithLoggingBase
{
    public QueryParserValidationTests(ITestOutputHelper output) : base(output)
    {
        Log.DefaultMinimumLevel = LogLevel.Trace;
    }

    [Theory]
    [InlineData("*")]
    [InlineData("\":\"")]
    [InlineData("\"quoted string\"")]
    [InlineData("criteria")]
    [InlineData("(criteria)")]
    [InlineData("field:criteria")]
    [InlineData("-criteria")]
    [InlineData("+criteria")]
    [InlineData("criteria1 AND NOT criteria2")]
    [InlineData("criteria1 NOT criteria2")]
    [InlineData("field:criteria1 NOT field:criteria2")]
    [InlineData("criteria1 +criteria2")]
    [InlineData("criteria1 OR criteria2")]
    [InlineData("criteria1 OR criteria2 OR criteria3")]
    [InlineData("criteria1 OR (criteria2 AND criteria3)")]
    [InlineData("field:[1 TO 2]")]
    [InlineData("field:{1 TO 2}")]
    [InlineData("field:[1 TO 2}")]
    [InlineData("field:(criteria1 criteria2)")]
    [InlineData("data.field:(now criteria2)")]
    [InlineData("field:(criteria1 OR criteria2)")]
    [InlineData("field:*cr")]
    [InlineData("field:c*r")]
    [InlineData("field:cr*")]
    [InlineData("date:>now")]
    [InlineData("date:<now")]
    [InlineData("_exists_:title")]
    [InlineData("_missing_:title")]
    [InlineData("book.\\*:(quick brown)")]
    [InlineData("date:[now/d-4d TO now/d+1d}")]
    [InlineData("(date:[now/d-4d TO now/d+1d})")]
    [InlineData("data.date:>now")]
    [InlineData("data.date:[now/d-4d TO now/d+1d}")]
    [InlineData("data.date:[2012-01-01 TO 2012-12-31]")]
    [InlineData("data.date:[* TO 2012-12-31]")]
    [InlineData("data.date:[2012-01-01 TO *]")]
    [InlineData("(data.date:[now/d-4d TO now/d+1d})")]
    [InlineData("criter~")]
    [InlineData("criter~1")]
    [InlineData("roam~0.8")]
    [InlineData("criter^2")]
    [InlineData("\"blah criter\"~1")]
    [InlineData("count:>1")]
    [InlineData(@"book.\*:test")]
    [InlineData("count:>=1")]
    [InlineData("count:[1..5}")]
    [InlineData(@"count:a\:a")]
    [InlineData(@"count:a\:a more:stuff")]
    [InlineData("data.count:[1..5}")]
    [InlineData("age:[1 TO 2]")]
    [InlineData("data.Windows-identity:ejsmith")]
    [InlineData("data.age:(>30 AND <=40)")]
    [InlineData("+>=10")]
    [InlineData(">=10")]
    [InlineData("age:(+>=10)")]
    [InlineData("data.age:(+>=10 +<20)")]
    [InlineData("data.age:[10 TO *]")]
    [InlineData("title:(full text search)^2")]
    [InlineData("data.age:[* TO 10]")]
    [InlineData("data:[* TO 10]^hey")]
    [InlineData("hidden:true AND data.age:(>30 AND <=40)")]
    [InlineData("hidden:true")]
    [InlineData("geo:\"Dallas, TX\"~75m")]
    [InlineData("geo:\"Dallas, TX\"~75 m")]
    [InlineData("min:price geogrid:geo~6 count:(category count:subcategory avg:price min:price)")]
    [InlineData("datehistogram:(date~2^-5\\:30 min:date)")]
    [InlineData("-type:404")]
    [InlineData("type:test?s")]
    [InlineData("(NOT someField:stuff)")]
    [InlineData("(NOT (someField:stuff))")]
    [InlineData("(NOT -(stuff))")]
    [InlineData("(NOT -someField:stuff)")]
    [InlineData("(NOT someField:(stuff))")]
    [InlineData("(NOT someField:(NOT stuff))")]
    [InlineData("(NOT -someField:(stuff))")]
    [InlineData("something AND NOT otherthing")]
    [InlineData("something AND otherthing")]
    [InlineData("something OR otherthing")]
    [InlineData("NOT Test")]
    [InlineData("! Test")]
    [InlineData("!Test")]
    [InlineData("One - Two")]
    [InlineData("One -Two")]
    [InlineData(@"type:\(11\)2\+")]
    [InlineData(@"""\""now""")]
    [InlineData("title:(+return +\"pink panther\")")]
    [InlineData("\"jakarta apache\" -\"Apache Lucene\"")]
    [InlineData("\"jakarta apache\"^4 \"Apache Lucene\"")]
    [InlineData("NOT \"jakarta apache\"")]
    [InlineData(@"updated:2016-09-02T15\:41\:43.3385286Z")]
    [InlineData(@"updated:>2016-09-02T15\:41\:43.3385286Z")]
    [InlineData(@"field1:""\""value1\""""")]
    [InlineData(@"""\""value1""")]
    [InlineData("Hello world")]
    [InlineData("Hello (world)")]
    [InlineData("Hello \"world\"")]
    [InlineData("+Hello +world")]
    [InlineData("ANDmammoth")]
    [InlineData("xy/z")] // not treated like a regex since the term does not start with a /
    [InlineData("quik~2c")]
    [InlineData("ab~2z")]
    [InlineData("Author:Smith AND Title_idx:\"\"")]
    [InlineData(@"first_occurrence:[""2022-01-20T14:00:00.0000000Z"" TO ""2022-01-21T02:33:06.5975418Z""] (project:537650f3b77efe23a47914f4 (status:open OR status:regressed))")]
    public Task ValidQueries(string query)
    {
        return ParseAndValidateQuery(query, query, true);
    }

    [Theory]
    [InlineData(":")]
    [InlineData("string\"")]
    [InlineData("field:*")]
    [InlineData("count:a:a")]
    [InlineData("type:?")]
    [InlineData("Hello (world")] // term grouping is left unterminated
    [InlineData("Hello \"world")] // qouted term is left unterminated
    [InlineData("Hello + world")]
    [InlineData("/abc")] // regex sequence is left unterminated
    public Task InvalidQueries(string query)
    {
        return ParseAndValidateQuery(query, query, false);
    }

    [Theory]
    [InlineData("field :criteria", "field:criteria")]
    [InlineData("data.age:(+>=10 AND < 20)", "data.age:(+>=10 AND <20)")]
    [InlineData("data.age:(->=10 AND < 20)", "data.age:(->=10 AND <20)")]
    [InlineData("age:(>=10 AND < 20)", "age:(>=10 AND <20)")]
    [InlineData("age : >= 10", "age:>=10")]
    [InlineData(@"( ( cat AND dog ))", @"((cat AND dog))")]
    [InlineData(@"date^""America/Chicago_Other""", @"date^America/Chicago_Other")]
    [InlineData("  \t", "")]
    public Task ValidButDifferentQueries(string query, string expected)
    {
        return ParseAndValidateQuery(query, expected, true);
    }

    private async Task ParseAndValidateQuery(string query, string expected, bool isValid)
    {
#if ENABLE_TRACING
        var tracer = new LoggingTracer(_logger, reportPerformance: true);
#else
            var tracer = NullTracer.Instance;
#endif
        var parser = new LuceneQueryParser
        {
            Tracer = tracer
        };

        IQueryNode result;
        try
        {
            result = await parser.ParseAsync(query);
        }
        catch (FormatException ex)
        {
            Assert.False(isValid, ex.Message);
            return;
        }

        string nodes = await DebugQueryVisitor.RunAsync(result);
        _logger.LogInformation(nodes);
        string generatedQuery = await GenerateQueryVisitor.RunAsync(result);
        Assert.Equal(expected, generatedQuery);
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
    [InlineData("/")]
    public async Task CanParseEscapedQuery(string escaped)
    {
        // https://www.elastic.co/guide/en/elasticsearch/reference/current/query-dsl-query-string-query.html#_reserved_characters
        // + - && || ! ( ) { } [ ] ^ " ~ * ? : \ /
        string query = @"\" + escaped;
#if ENABLE_TRACING
        var tracer = new LoggingTracer(_logger, reportPerformance: true);
#else
            var tracer = NullTracer.Instance;
#endif
        var parser = new LuceneQueryParser
        {
            Tracer = tracer
        };

        try
        {
            _logger.LogInformation($"Attempting: {escaped}");
            var result = await parser.ParseAsync(query);

            _logger.LogInformation(await DebugQueryVisitor.RunAsync(result));
            string generatedQuery = await GenerateQueryVisitor.RunAsync(result);
            Assert.Equal(query, generatedQuery);
        }
        catch (FormatException ex)
        {
            var cursor = ex.Data["cursor"] as Cursor;
            throw new FormatException($"[{cursor.Line}:{cursor.Column}] {ex.Message}", ex);
        }
    }
}
