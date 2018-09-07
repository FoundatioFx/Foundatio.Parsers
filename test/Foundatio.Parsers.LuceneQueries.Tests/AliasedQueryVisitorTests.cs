using System;
using System.Linq;
using System.Threading.Tasks;
using Foundatio.Parsers.ElasticQueries;
using Foundatio.Parsers.LuceneQueries;
using Foundatio.Parsers.LuceneQueries.Visitors;
using Nest;
using Xunit;

namespace Foundatio.Parsers.Tests {
    public class AliasedQueryVisitorTests {
        [Fact]
        public async Task CanUseAliasMapForTopLevelAliasAsync() {
            var parser = new LuceneQueryParser();
            var result = await parser.ParseAsync("field1:value");
            var aliasMap = new AliasMap { { "field1", "field2" } };
            var aliased = await AliasedQueryVisitor.RunAsync(result, aliasMap);
            Assert.Equal("field2:value", aliased.ToString());
        }

        [Fact]
        public async Task CanUseAliasMapForTopLevelAlias2Async() {
            string filter = "program:postgrad";
            var aliasMap = new AliasMap {
               { "program", "programName" }
            };

            var p = new ElasticQueryParser(c => c.UseAliases(aliasMap));
            IQueryContainer query = await p.BuildQueryAsync(filter);
            var term = query.Bool.Filter.Single() as IQueryContainer;
            Assert.NotNull(term.Term);
            Assert.Equal("programName", term.Term.Field.Name);
            Assert.Equal("postgrad", term.Term.Value);
        }

        [Fact]
        public async Task AliasMapShouldBeAppliedToAllLevels3Async() {
            var parser = new LuceneQueryParser();
            var result = await parser.ParseAsync("field1.nested:value");
            var aliasMap = new AliasMap {
                { "nested", "field2" }
            };
            var aliased = await AliasedQueryVisitor.RunAsync(result, aliasMap);
            Assert.Equal("field1.nested:value", aliased.ToString());
        }

        [Fact]
        public async Task AliasMapShouldBeAppliedToAllLevels4Async() {
            var parser = new LuceneQueryParser();
            var result = await parser.ParseAsync("field1.nested:value");
            var aliasMap = new AliasMap {
                { "field1.nested", "field2.other" }
            };
            var aliased = await AliasedQueryVisitor.RunAsync(result, aliasMap);
            Assert.Equal("field2.other:value", aliased.ToString());
        }

        [Fact]
        public async Task AliasMapShouldBeAppliedToAllLevelsAsync() {
            var parser = new LuceneQueryParser();
            var result = await parser.ParseAsync("field1.nested:value");
            var aliasMap = new AliasMap {
                { "field1", new AliasMapValue { Name = "field2", ChildMap = { { "nested", "other" } } } }
            };
            var aliased = await AliasedQueryVisitor.RunAsync(result, aliasMap);
            Assert.Equal("field2.other:value", aliased.ToString());
        }

        [Fact]
        public async Task AliasMapShouldBeAppliedToAllLevels7Async() {
            var parser = new LuceneQueryParser();
            var result = await parser.ParseAsync("field1.nested.childproperty:value");
            var aliasMap = new AliasMap {
                { "field1", new AliasMapValue { Name = "field2", ChildMap = { { "nested", "other" } } } }
            };
            var aliased = await AliasedQueryVisitor.RunAsync(result, aliasMap);
            Assert.Equal("field2.other.childproperty:value", aliased.ToString());
        }

        [Fact]
        public async Task AliasMapShouldBeAppliedToAllLevels8Async() {
            var parser = new LuceneQueryParser();
            var result = await parser.ParseAsync("field1:(nested.childproperty:value)");
            var aliasMap = new AliasMap {
                { "field1", new AliasMapValue { Name = "field2", ChildMap = { { "nested", "other" } } } }
            };
            var aliased = await AliasedQueryVisitor.RunAsync(result, aliasMap);
            Assert.Equal("field2:(other.childproperty:value)", aliased.ToString());
        }

        [Fact]
        public async Task AliasMapShouldBeAppliedToAllLevels6Async() {
            var parser = new LuceneQueryParser();
            var result = await parser.ParseAsync("field1.nested:(hey:value)");
            var aliasMap = new AliasMap {
                { "field1", new AliasMapValue { Name = "field2", ChildMap = { { "nested", new AliasMapValue { Name = "other", ChildMap = { { "hey", "blah" } } } } } } }
            };
            var aliased = await AliasedQueryVisitor.RunAsync(result, aliasMap);
            Assert.Equal("field2.other:(blah:value)", aliased.ToString());
        }

        [Fact]
        public async Task AliasMapShouldBeAppliedToAllLevels2Async() {
            var parser = new LuceneQueryParser();
            var result = await parser.ParseAsync("field1:(nested:value another:blah)");
            var aliasMap = new AliasMap {
                { "field1", new AliasMapValue { Name = "field2", ChildMap = { { "nested", "other" } } } }
            };
            var aliased = await AliasedQueryVisitor.RunAsync(result, aliasMap);
            Assert.Equal("field2:(other:value another:blah)", aliased.ToString());
        }

        [Fact]
        public async Task AliasMapShouldAllowDeepAliasesAsync() {
            var parser = new LuceneQueryParser();
            var result = await parser.ParseAsync("level1.level2.level3:(level4:value)");
            var aliasMap = new AliasMap {
                { "level1", new AliasMapValue { Name = "alias1", ChildMap = { { "level2", "alias2" }, { "level4", "alias4" } } } }
            };
            var aliased = await AliasedQueryVisitor.RunAsync(result, aliasMap);
            Assert.Equal("alias1.alias2.level3:(level4:value)", aliased.ToString());
        }

        [Fact]
        public async Task AliasMapShouldNotApplyRootAliasesToNestedTermAsync() {
            var parser = new LuceneQueryParser();
            var result = await parser.ParseAsync("field1.nested:value");
            var aliasMap = new AliasMap {
                { "field1", new AliasMapValue { Name = "field2", ChildMap = { { "stuff", "other" } } } }
            };
            var aliased = await AliasedQueryVisitor.RunAsync(result, aliasMap);
            Assert.Equal("field2.nested:value", aliased.ToString());
        }

        [Fact]
        public async Task CanApplyRootLevelAliasMapOnNestedTermAsync() {
            var parser = new LuceneQueryParser();
            var result = await parser.ParseAsync("field1.nested.morenested:value");
            var aliasMap = new AliasMap {
                { "field1", new AliasMapValue { Name = "field2", ChildMap = { { "stuff", "other" } } } }
            };
            var aliased = await AliasedQueryVisitor.RunAsync(result, aliasMap);
            Assert.Equal("field2.nested.morenested:value", aliased.ToString());
        }

        [Fact]
        public async Task AliasMapShouldWorkOnGroupsAsync() {
            var parser = new LuceneQueryParser();
            var result = await parser.ParseAsync("field1:(nested:value OR thing:yep) another:works");
            var aliasMap = new AliasMap {
                {
                    "field1",
                    new AliasMapValue { Name = "field2", ChildMap = { { "nested", "other" }, { "thing", "nice" } } }
                }
            };
            var aliased = await AliasedQueryVisitor.RunAsync(result, aliasMap);
            Assert.Equal("field2:(other:value OR nice:yep) another:works", aliased.ToString());
        }

        [Fact]
        public async Task CanUseResolverAsync() {
            var parser = new LuceneQueryParser();
            var result = await parser.ParseAsync("field1.nested:value");
            var aliased = await AliasedQueryVisitor.RunAsync(result, f => f == "field1.nested" ? new GetAliasResult { Name = "field2.nested" } : null);
            Assert.Equal("field2.nested:value", aliased.ToString());
        }
    }
}
