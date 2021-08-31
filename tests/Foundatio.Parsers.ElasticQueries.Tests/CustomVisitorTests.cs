using System;
using System.Collections.Generic;
using System.Text;
using System.Threading.Tasks;
using Foundatio.Parsers.ElasticQueries.Extensions;
using Foundatio.Parsers.LuceneQueries.Nodes;
using Foundatio.Parsers.LuceneQueries.Visitors;
using Microsoft.Extensions.Logging;
using Nest;
using Xunit;
using Xunit.Abstractions;

namespace Foundatio.Parsers.ElasticQueries.Tests {
    public class CustomVisitorTests : ElasticsearchTestBase {

        public CustomVisitorTests(ITestOutputHelper output) : base(output) {
            Log.MinimumLevel = Microsoft.Extensions.Logging.LogLevel.Trace;
        }

        [Fact]
        public void CanResolveSimpleCustomFilter() {
            var client = GetClient();
            var index = CreateRandomIndex<MyType>(client);
            client.Index(new MyType { Id = "1" }, i => i.Index(index));

            var processor = new ElasticQueryParser(c => c
                .SetLoggerFactory(Log)
                .AddVisitor(new IncludeVisitor())
                .AddVisitor(new CustomFilterVisitor()));
            
            var result = processor.BuildQueryAsync("@custom:(one)").Result;
            var actualResponse = client.Search<MyType>(d => d.Index(index).Query(q => result));
            string actualRequest = actualResponse.GetRequest();
            _logger.LogInformation("Actual: {Request}", actualRequest);

            var expectedResponse = client.Search<MyType>(d => d
                .Index(index).Query(f => f.Bool(b => b.Filter(filter => filter.Terms(m => m.Field("id").Terms("1"))))));
            string expectedRequest = expectedResponse.GetRequest();
            _logger.LogInformation("Expected: {Request}", expectedRequest);

            Assert.Equal(expectedRequest, actualRequest);
            Assert.Equal(expectedResponse.Total, actualResponse.Total);
        }
        
        [Fact]
        public void CanResolveCustomFilterContainingIncludes() {
            var client = GetClient();
            var index = CreateRandomIndex<MyType>(client);
            client.Index(new MyType { Id = "1" }, i => i.Index(index));

            var processor = new ElasticQueryParser(c => c
                .SetLoggerFactory(Log)
                .AddVisitor(new IncludeVisitor())
                .AddVisitor(new CustomFilterVisitor()));
            
            var result = processor.BuildQueryAsync("@custom:(one @include:3)").Result;
            var actualResponse = client.Search<MyType>(d => d.Index(index).Query(q => result));
            string actualRequest = actualResponse.GetRequest();
            _logger.LogInformation("Actual: {Request}", actualRequest);

            var expectedResponse = client.Search<MyType>(d => d
                .Index(index).Query(f => f.Bool(b => b.Filter(filter => filter.Terms(m => m.Field("id").Terms("1", "3"))))));
            string expectedRequest = expectedResponse.GetRequest();
            _logger.LogInformation("Expected: {Request}", expectedRequest);

            Assert.Equal(expectedRequest, actualRequest);
            Assert.Equal(expectedResponse.Total, actualResponse.Total);
        }
        
        [Fact]
        public void CanResolveIncludeToCustomFilterContainingIgnoredInclude() {
            var client = GetClient();
            var index = CreateRandomIndex<MyType>(client);
            client.Index(new MyType { Id = "1" }, i => i.Index(index));

            var processor = new ElasticQueryParser(c => c
                .SetLoggerFactory(Log)
                .UseIncludes(include => ResolveIncludeAsync("test", include, "@custom:(one @include:3)"), ShouldSkipInclude, 0)
                .AddVisitor(new CustomFilterVisitor(), 1));
            
            var result = processor.BuildQueryAsync("@include:test").Result;
            var actualResponse = client.Search<MyType>(d => d.Index(index).Query(q => result));
            string actualRequest = actualResponse.GetRequest();
            _logger.LogInformation("Actual: {Request}", actualRequest);

            var expectedResponse = client.Search<MyType>(d => d
                .Index(index).Query(f => f.Bool(b => b.Filter(filter => filter.Terms(m => m.Field("id").Terms("1", "3"))))));
            string expectedRequest = expectedResponse.GetRequest();
            _logger.LogInformation("Expected: {Request}", expectedRequest);

            Assert.Equal(expectedRequest, actualRequest);
            Assert.Equal(expectedResponse.Total, actualResponse.Total);
        }

        private bool ShouldSkipInclude(TermNode node, IQueryVisitorContext context) {
            var current = node.Parent;
            while (current != null) {
                if (current is GroupNode groupNode && groupNode.Field == "@custom")
                    return true;
                
                current = current.Parent;
            }

            return false;
        }

        private static Task<string> ResolveIncludeAsync(string expected, string actual, string resolvedFilterIfMatch) {
            if (String.Equals(expected, actual))
                return Task.FromResult(resolvedFilterIfMatch);
            
            throw new ArgumentException(nameof(actual), $"Unable to resolve filter with id: {actual}");
        }

        [Fact]
        public void CanResolveMultipleCustomFilters() {
            var client = GetClient();
            var index = CreateRandomIndex<MyType>(client);
            client.Index(new MyType { Id = "1" }, i => i.Index(index));

            var processor = new ElasticQueryParser(c => c
                .SetLoggerFactory(Log)
                .AddVisitor(new IncludeVisitor())
                .AddVisitor(new CustomFilterVisitor()));
            
            var result = processor.BuildQueryAsync("@custom:(one) OR (field1:Test @custom:(two))").Result;
            var actualResponse = client.Search<MyType>(d => d.Index(index).Query(q => result));
            string actualRequest = actualResponse.GetRequest();
            _logger.LogInformation("Actual: {Request}", actualRequest);
            
            var expectedResponse = client.Search<MyType>(d => d.Index(index)
                .Query(f => f
                    .Bool(b => b
                        .Filter(filter => filter
                            .Bool(b1 => b1
                                .Should(
                                    s1 => s1.Terms(m => m.Field("id").Terms("1")),
                                    s2 => s2.Bool(b2 => b2
                                        .Must(
                                            m2 => m2.Terms(t1 => t1.Field("id").Terms("2")),
                                            m2 => m2.Term(t1 => t1.Field("field1").Value("Test"))
                                        )
                                    )
                                )
                            )
                        )
                    )
                )
            );
            
            string expectedRequest = expectedResponse.GetRequest();
            _logger.LogInformation("Expected: {Request}", expectedRequest);

            Assert.Equal(expectedRequest, actualRequest);
            Assert.Equal(expectedResponse.Total, actualResponse.Total);
        }
    }

    /// <summary>
    /// Let's resolve a custom id based on a node groups filter: @custom:(filter)
    /// </summary>
    public sealed class CustomFilterVisitor : ChainableQueryVisitor {
        public override async Task VisitAsync(GroupNode node, IQueryVisitorContext context) {
            if (node.Field == "@custom" && node.Left != null) {
                string term = ToTerm(node);
                var ids = await GetIdsAsync(term);
                if (ids != null && ids.Count > 0)
                    node.Parent.SetQuery(new TermsQuery { Field = "id", Terms = ids });
                else 
                    node.Parent.SetQuery(new TermQuery { Field = "id", Value = "none" });
                
                node.Left = null;
                node.Right = null;
            }
            
            await base.VisitAsync(node, context);
        }

        private static Task<List<string>> GetIdsAsync(string term) {
            var ids = new List<string>();
            switch (term?.ToLowerInvariant()) {
                case "one":
                    ids.Add("1");
                    break;
                case "two":
                    ids.Add("2");
                    break;
                case "one @include:3":
                    ids.AddRange(new [] { "1", "3"});
                    break;
            }

            return Task.FromResult(ids);
        }
        
        private static string ToTerm(GroupNode node) {
            var builder = new StringBuilder();

            if (node.Left != null)
                builder.Append(node.Left);
            
            if (node.Right != null) {
                builder.Append(" ");
                builder.Append(node.Right);
            }

            return builder.ToString();
        }
    }
}