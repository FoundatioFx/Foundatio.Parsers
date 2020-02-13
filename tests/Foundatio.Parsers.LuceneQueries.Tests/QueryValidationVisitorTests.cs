using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using Foundatio.Logging.Xunit;
using Foundatio.Parsers.ElasticQueries;
using Foundatio.Parsers.ElasticQueries.Visitors;
using Foundatio.Parsers.LuceneQueries;
using Foundatio.Parsers.LuceneQueries.Nodes;
using Foundatio.Parsers.LuceneQueries.Visitors;
using Microsoft.Extensions.Logging;
using Nest;
using Xunit;
using Xunit.Abstractions;

namespace Foundatio.Parsers.Tests {
    public class QueryValidationVisitorTests : TestWithLoggingBase {
        private readonly IQueryParser _luceneQueryParser = new LuceneQueryParser();
        private readonly IQueryParser _elasticQueryParser;

        public QueryValidationVisitorTests(ITestOutputHelper output) : base(output) {
            Log.MinimumLevel = Microsoft.Extensions.Logging.LogLevel.Trace;
            _elasticQueryParser = new ElasticQueryParser(c => c.SetLoggerFactory(Log));
        }

        [Fact]
        public Task GetLuceneQueryInfoAsync() {
            return GetQueryInfoAsync(_luceneQueryParser);
        }

        [Fact]
        public Task GetElasticQueryInfoAsync() {
            return GetQueryInfoAsync(_elasticQueryParser);
        }

        private async Task GetQueryInfoAsync(IQueryParser parser) {
            const string query = "stuff hey:now nested:(stuff:33)";
            var result = await parser.ParseAsync(query);

            var info = await ValidationVisitor.RunAsync(result);
            Assert.Equal(QueryType.Query, info.QueryType);
            Assert.Equal(2, info.MaxNodeDepth);
            Assert.Equal(new HashSet<string> {
                "",
                "hey",
                "nested",
                "stuff"
            }, info.ReferencedFields);
        }

        public static IEnumerable<object[]> AggregationTestCases {
            get {
                return new[] {
                    new object[] { null, false, 1, new HashSet<string>(), new Dictionary<string, ICollection<string>>() },
                    new object[] { "avg", false, 1, new HashSet<string> { ""}, new Dictionary<string, ICollection<string>>() },
                    new object[] { "avg:", false, 1, new HashSet<string>(), new Dictionary<string, ICollection<string>>() },
                    new object[] { "avg:value", true, 1,
                        new HashSet<string> { "value" },
                        new Dictionary<string, ICollection<string>> { { "avg", new HashSet<string> { "value" } } }
                    },
                    new object[] { "    avg    :    value", true, 1,
                        new HashSet<string> { "value"},
                        new Dictionary<string, ICollection<string>> { { "avg", new HashSet<string> { "value" } } }
                    },
                    new object[] { "avg:value cardinality:value sum:value min:value max:value", true, 1,
                        new HashSet<string> { "value" },
                        new Dictionary<string, ICollection<string>> {
                            { "avg", new HashSet<string> { "value" } },
                            { "cardinality", new HashSet<string> { "value" } },
                            { "sum", new HashSet<string> { "value" } },
                            { "min", new HashSet<string> { "value" } },
                            { "max", new HashSet<string> { "value" } }
                        }
                    },
                    new object[] { "avg:value avg:value2", true, 1,
                        new HashSet<string> { "value", "value2" },
                        new Dictionary<string, ICollection<string>> { { "avg", new HashSet<string> { "value", "value2" } } }
                    },
                    new object[] { "avg:value avg:value", true, 1,
                        new HashSet<string> { "value" },
                        new Dictionary<string, ICollection<string>> { { "avg", new HashSet<string> { "value" } } }
                    }
                };
            }
        }

        [Theory]
        [MemberData(nameof(AggregationTestCases))]
        public Task GetElasticAggregationQueryInfoAsync(string query, bool isValid, int maxNodeDepth, HashSet<string> fields, Dictionary<string, ICollection<string>> operations) {
            return GetAggregationQueryInfoAsync(_elasticQueryParser, query, isValid, maxNodeDepth, fields, operations);
        }

        private async Task GetAggregationQueryInfoAsync(IQueryParser parser, string query, bool isValid, int maxNodeDepth, HashSet<string> fields, Dictionary<string, ICollection<string>> operations) {
            IQueryNode queryNode;
            var context = new ElasticQueryVisitorContext { QueryType = QueryType.Aggregation };
            try {
                queryNode = await parser.ParseAsync(query, context);
            } catch (Exception ex) {
                _logger.LogError(ex, "Error parsing query: {Message}", ex.Message);
                if (isValid)
                    throw;

                return;
            }
            
            var info = await ValidationVisitor.RunAsync(queryNode, context);
            Assert.Equal(QueryType.Aggregation, info.QueryType);
            Assert.Equal(isValid, info.IsValid);
            Assert.Equal(maxNodeDepth, info.MaxNodeDepth);
            Assert.Equal(fields, info.ReferencedFields);
            Assert.Equal(operations, info.Operations.ToDictionary(pair => pair.Key, pair => pair.Value));
        }
    }
}
