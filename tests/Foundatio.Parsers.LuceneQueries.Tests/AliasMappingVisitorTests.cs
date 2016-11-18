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
        }

        [Fact]
        public void VerifyNestedAlias() {
            var client = GetClient();
            var visitor = new AliasMappingVisitor(client.Infer);
            var walker = new MappingWalker(visitor);
            walker.Accept(new TypeMappingDescriptor<Employee>().Properties(p => p
                .Keyword(f => f.Name(e => e.Id).Alias("employee_id"))
                .Object<object>(o => o.Name("Data").Properties(p1 => p1
                    .Keyword(f => f.Name("Profile_URL").Alias("url"))))));

            var map = visitor.RootAliasMap;
            Assert.Equal(2, map.Count);
            Assert.True(map.ContainsKey("employee_id"));
            Assert.Equal("id", map["employee_id"].Name);
            Assert.False(map["employee_id"].HasChildMappings);

            Assert.True(map.ContainsKey("Data"));
            Assert.Equal("Data", map["Data"].Name);
            Assert.True(map["Data"].HasChildMappings);
            Assert.Equal(1, map["Data"].ChildMap.Count);
            Assert.True(map["Data"].ChildMap.ContainsKey("url"));
            Assert.Equal("Profile_URL", map["Data"].ChildMap["url"].Name);
            Assert.False(map["Data"].ChildMap["url"].HasChildMappings);
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
                    .Object<object>(o => o.Name("Data").Properties(p1 => p1
                        .Keyword(f => f.Name("Profile_URL").Alias("url")))));

            await client.CreateIndexAsync(index, d => d.Mappings(m => m.Map<Employee>(index, md => mapping)));
            var response = client.IndexMany(new List<Employee> {
                new Employee { Id = "ex-001", Data = new Dictionary<string, object> { { "Profile_URL", "http://contoso.com/u/ex-001/profile.png" } } },
            }, index);
            await client.RefreshAsync(index);

            var visitor = new AliasMappingVisitor(client.Infer);
            var walker = new MappingWalker(visitor);
            walker.Accept(mapping);
            
            var processor = new ElasticQueryParser(c => c.UseAliases(visitor.RootAliasMap).UseMappings<Employee>(m => mapping, () => client.GetMapping(new GetMappingRequest(index, index)).Mapping));
            var result = await processor.BuildQueryAsync("employee_id:ex-001 data.url:profile.png url:\"http://contoso.com/u/ex-001/profile.png\"");
            var actualResponse = client.Search<Employee>(d => d.Index(index).Query(q => result));
            string actualRequest = actualResponse.GetRequest();
            _logger.Info($"Actual: {actualRequest}");

            var expectedResponse = client.Search<Employee>(d => d.Index(index).Type(index).Query(q => q
                .Bool(b => b.Filter(
                    Query<Employee>.Term(f1 => f1.Id, "ex-001") && 
                    Query<Employee>.Term(f1 => f1.Data["Profile_URL"], "profile.png") && 
                    Query<Employee>.Term(f1 => f1.Data["Profile_URL"], "http://contoso.com/u/ex-001/profile.png")))
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