using System;
using System.Collections.Generic;
using System.Threading.Tasks;
using Foundatio.Logging;
using Foundatio.Logging.Xunit;
using Foundatio.Parsers.ElasticQueries;
using Foundatio.Parsers.ElasticQueries.Extensions;
using Foundatio.Parsers.ElasticQueries.Visitors;
using Nest;
using Xunit;
using Xunit.Abstractions;
using Foundatio.Parsers.LuceneQueries;
using Foundatio.Parsers.LuceneQueries.Visitors;
using Foundatio.Parsers.Tests.Extensions;

namespace Foundatio.Parsers.Tests {
    public sealed class AliasMappingVisitorTests : TestWithLoggingBase {
        public AliasMappingVisitorTests(ITestOutputHelper output) : base(output) {}

        private IElasticClient GetClient(ConnectionSettings settings = null) {
            if (settings == null)
                settings = new ConnectionSettings();

            return new ElasticClient(settings.DisableDirectStreaming().PrettyJson());
        }

        [Fact]
        public void VerifySimpleAlias() {
            var client = GetClient();
            var visitor = new AliasMappingVisitor(client.Infer);
            var walker = new MappingWalker(visitor);
            walker.Accept(new TypeMappingDescriptor<Employee>().Properties(p => p
                .Keyword(f => f.Name(e => e.Id).Alias("employee_id"))));

            var map = visitor.RootAliasMap;
            Assert.Equal(1, map.Count);
            Assert.True(map.ContainsKey("employee_id"));
            Assert.Equal("id", map["employee_id"].Name);
            Assert.False(map["employee_id"].HasChildMappings);

            var parser = new LuceneQueryParser();
            var result = parser.Parse("employee_id:1234");
            var aliased = AliasedQueryVisitor.Run(result, map);
            Assert.Equal("id:1234", aliased.ToString());
        }

        [Fact]
        public void VerifyNestedAlias() {
            var client = GetClient();
            var visitor = new AliasMappingVisitor(client.Infer);
            var walker = new MappingWalker(visitor);
            walker.Accept(new TypeMappingDescriptor<Employee>().Properties(p => p
                .Keyword(f => f.Name(e => e.Id).Alias("employee_id"))
                .Object<object>(o => o.Name("data").Properties(p1 => p1
                    .Keyword(f => f.Name("Profile_URL").Alias("url"))))));

            var map = visitor.RootAliasMap;
            Assert.Equal(2, map.Count);
            Assert.True(map.ContainsKey("employee_id"));
            Assert.Equal("id", map["employee_id"].Name);
            Assert.False(map["employee_id"].HasChildMappings);

            Assert.True(map.ContainsKey("data"));
            Assert.Equal("data", map["data"].Name);
            Assert.True(map["data"].HasChildMappings);
            Assert.Equal(1, map["data"].ChildMap.Count);
            Assert.True(map["data"].ChildMap.ContainsKey("url"));
            Assert.Equal("Profile_URL", map["data"].ChildMap["url"].Name);
            Assert.False(map["data"].ChildMap["url"].HasChildMappings);

            var parser = new LuceneQueryParser();
            var result = parser.Parse("employee_id:1234 data.url:test");
            var aliased = AliasedQueryVisitor.Run(result, map);
            Assert.Equal("id:1234 data.Profile_URL:test", aliased.ToString());
        }

        [Fact]
        public void VerifyRootNestedAlias() {
            var client = GetClient();
            var visitor = new AliasMappingVisitor(client.Infer);
            var walker = new MappingWalker(visitor);
            walker.Accept(new TypeMappingDescriptor<Employee>().Properties(p => p
                .Object<object>(o => o.Name("data").Properties(p1 => p1
                    .Keyword(f => f.Name("Profile_URL").RootAlias("url"))))));

            var map = visitor.RootAliasMap;
            Assert.Equal(2, map.Count);
            Assert.True(map.ContainsKey("url"));
            Assert.Equal("data.Profile_URL", map["url"].Name);
            Assert.False(map["url"].HasChildMappings);

            Assert.True(map.ContainsKey("data"));
            Assert.Equal("data", map["data"].Name);
            Assert.False(map["data"].HasChildMappings);

            var parser = new LuceneQueryParser();
            var result = parser.Parse("url:test");
            var aliased = AliasedQueryVisitor.Run(result, map);
            Assert.Equal("data.Profile_URL:test", aliased.ToString());
        }

        [Fact]
        public void VerifyComplexNestedAlias() {
            var client = GetClient();
            var visitor = new AliasMappingVisitor(client.Infer);
            var walker = new MappingWalker(visitor);
            walker.Accept(new TypeMappingDescriptor<Employee>().Properties(p => p
                .Object<object>(o1 => o1.Name("data").Properties(p1 => p1
                    .Object<object>(o2 => o2.Name("@request_info").Properties(p2 => p2
                        .Keyword(f => f.Name("user_agent").RootAlias("useragent"))
                        .Text(f4 => f4.Name("@browser_major_version").RootAlias("browser.major")
                            .Fields(sf => sf.Keyword(s => s.Name("keyword").IgnoreAbove(256))))
                        .Object<object>(o3 => o3.Name("data").Properties(p3 => p3
                            .Keyword(f => f.Name("@is_bot").RootAlias("bot"))))))))));

            var map = visitor.RootAliasMap;
            Assert.Equal(4, map.Count);
            Assert.True(map.ContainsKey("useragent"));
            Assert.Equal("data.@request_info.user_agent", map["useragent"].Name);
            Assert.False(map["useragent"].HasChildMappings);

            Assert.True(map.ContainsKey("browser.major"));
            Assert.Equal("data.@request_info.@browser_major_version", map["browser.major"].Name);
            Assert.False(map["browser.major"].HasChildMappings);

            Assert.True(map.ContainsKey("bot"));
            Assert.Equal("data.@request_info.data.@is_bot", map["bot"].Name);
            Assert.False(map["bot"].HasChildMappings);

            Assert.True(map.ContainsKey("data"));
            Assert.Equal("data", map["data"].Name);
            Assert.True(map["data"].HasChildMappings);

            var parser = new LuceneQueryParser();
            var result = parser.Parse("useragent:test browser.major:10 bot:true");
            var aliased = AliasedQueryVisitor.Run(result, map);
            Assert.Equal("data.@request_info.user_agent:test data.@request_info.@browser_major_version:10 data.@request_info.data.@is_bot:true", aliased.ToString());
        }

        [Fact]
        public async Task CanQueryByAliasAsync() {
            string index = nameof(Employee).ToLower();

            var client = GetClient();
            await client.DeleteIndexAsync(index);
            await client.RefreshAsync(index);

            var mapping = new TypeMappingDescriptor<Employee>()
                .Properties(p => p
                    .Keyword(f => f.Name(e => e.Id).Alias("employee_id"))
                    .Object<object>(o => o.Name("data").Properties(p1 => p1
                        .Keyword(f => f.Name("Profile_URL").RootAlias("url"))
                        .Keyword(f => f.Name("Security_Access_Code").Alias("code")))));

            await client.CreateIndexAsync(index, d => d.Mappings(m => m.Map<Employee>(index, md => mapping)));
            var response = client.IndexMany(new List<Employee> {
                new Employee { Id = "ex-001", Data = new Dictionary<string, object> {
                    { "Profile_URL", "/u/ex-001/profile.png" },
                    { "Security_Access_Code", "1234567890" }
                } },
            }, index);
            await client.RefreshAsync(index);

            var visitor = new AliasMappingVisitor(client.Infer);
            var walker = new MappingWalker(visitor);
            walker.Accept(mapping);
            
            var processor = new ElasticQueryParser(c => c.UseAliases(visitor.RootAliasMap).UseMappings<Employee>(m => mapping, () => client.GetMapping(new GetMappingRequest(index, index)).Mapping));
            var result = await processor.BuildQueryAsync("employee_id:ex-001 url:\"/u/ex-001/profile.png\" data.code:1234567890");
            var actualResponse = client.Search<Employee>(d => d.Index(index).Query(q => result));
            string actualRequest = actualResponse.GetRequest();
            _logger.Info($"Actual: {actualRequest}");

            var expectedResponse = client.Search<Employee>(d => d.Index(index).Type(index).Query(q => q
                .Bool(b => b.Filter(
                    Query<Employee>.Term(f1 => f1.Id, "ex-001") &&
                    Query<Employee>.Term(f1 => f1.Data["Profile_URL"], "/u/ex-001/profile.png") &&
                    Query<Employee>.Term(f1 => f1.Data["Security_Access_Code"], "1234567890")))
                ));

            string expectedRequest = expectedResponse.GetRequest();
            _logger.Info($"Expected: {expectedRequest}");

            Assert.Equal(expectedRequest, actualRequest);
            Assert.True(actualResponse.IsValid);
            Assert.True(expectedResponse.IsValid);
            Assert.Equal(expectedResponse.Total, actualResponse.Total);
        }

        private class Employee {
            public string Id { get; set; }
            public Dictionary<string, object> Data { get; set; }
        }
    }
}