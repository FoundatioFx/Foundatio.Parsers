using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using Foundatio.Parsers.ElasticQueries;
using Foundatio.Parsers.LuceneQueries;
using Foundatio.Parsers.LuceneQueries.Visitors;
using Nest;
using Xunit;

namespace Foundatio.Parsers.Tests {
    public class FieldResolverVisitorTests {
        [Fact]
        public async Task CanUseAliasMapForTopLevelAliasAsync() {
            var parser = new LuceneQueryParser();
            var result = await parser.ParseAsync("field1:value");
            var aliasMap = new FieldMap { { "field1", "field2" } };
            var aliased = await FieldResolverQueryVisitor.RunAsync(result, aliasMap);
            Assert.Equal("field2:value", aliased.ToString());
        }

        [Fact]
        public async Task CanUseAliasMapForTopLevelAlias2Async() {
            string filter = "program:postgrad";
            var aliasMap = new FieldMap {
               { "program", "programName" }
            };

            var p = new ElasticQueryParser(c => c.UseFieldMap(aliasMap));
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
            var aliasMap = new FieldMap {
                { "nested", "field2" }
            };
            var aliased = await FieldResolverQueryVisitor.RunAsync(result, aliasMap);
            Assert.Equal("field1.nested:value", aliased.ToString());
        }

        [Fact]
        public async Task AliasMapShouldBeAppliedToAllLevels4Async() {
            var parser = new LuceneQueryParser();
            var result = await parser.ParseAsync("field1.nested:value");
            var aliasMap = new FieldMap {
                { "field1.nested", "field2.other" }
            };
            var aliased = await FieldResolverQueryVisitor.RunAsync(result, aliasMap);
            Assert.Equal("field2.other:value", aliased.ToString());
        }

        [Fact]
        public async Task AliasMapShouldBeAppliedToAllLevelsAsync() {
            var parser = new LuceneQueryParser();
            var result = await parser.ParseAsync("field1.nested:value");
            var aliasMap = new FieldMap {
                { "field1.nested", "field2.other" }
            };
            var aliased = await FieldResolverQueryVisitor.RunAsync(result, aliasMap);
            Assert.Equal("field2.other:value", aliased.ToString());
        }

        [Fact]
        public async Task AliasMapShouldBeAppliedToAllLevels7Async() {
            var parser = new LuceneQueryParser();
            var result = await parser.ParseAsync("field1.nested.childproperty:value");
            var aliasMap = new FieldMap {
                { "field1.nested.childproperty", "field2.other.childproperty" }
            };
            var aliased = await FieldResolverQueryVisitor.RunAsync(result, aliasMap);
            Assert.Equal("field2.other.childproperty:value", aliased.ToString());
        }

        [Fact]
        public async Task AliasMapShouldBeAppliedToAllLevels8Async() {
            var parser = new LuceneQueryParser();
            var result = await parser.ParseAsync("field1:(nested.childproperty:value)");
            var aliasMap = new FieldMap {
                { "field1.nested.childproperty", "field2.other.childproperty" }
            };
            var aliased = await FieldResolverQueryVisitor.RunAsync(result, aliasMap);
            Assert.Equal("(field2.other.childproperty:value)", aliased.ToString());
        }

        [Fact]
        public async Task AliasMapShouldBeAppliedToAllLevels6Async() {
            var parser = new LuceneQueryParser();
            var result = await parser.ParseAsync("field1.nested:(hey:value)");
            var aliasMap = new FieldMap {
                { "field1.nested.hey", "field2.other.blah" }
            };
            var aliased = await FieldResolverQueryVisitor.RunAsync(result, aliasMap);
            Assert.Equal("(field2.other.blah:value)", aliased.ToString());
        }

        [Fact]
        public async Task AliasMapShouldBeAppliedToAllLevels2Async() {
            var parser = new LuceneQueryParser();
            var result = await parser.ParseAsync("field1:(nested:value another:blah)");
            var aliasMap = new FieldMap {
                { "field1.nested", "field2.other" }
            };
            var aliased = await FieldResolverQueryVisitor.RunAsync(result, aliasMap);
            Assert.Equal("(field2.other:value field1.another:blah)", aliased.ToString());
        }

        [Fact]
        public async Task AliasMapShouldAllowDeepAliasesAsync() {
            var parser = new LuceneQueryParser();
            var result = await parser.ParseAsync("level1.level2.level3:(level4:value)");
            var aliasMap = new FieldMap {
                { "level1.level2.level3.level4", "alias1.alias2.level3.level4" }
            };
            var aliased = await FieldResolverQueryVisitor.RunAsync(result, aliasMap);
            Assert.Equal("(alias1.alias2.level3.level4:value)", aliased.ToString());
        }

        [Fact]
        public async Task AliasMapShouldNotApplyRootAliasesToNestedTermAsync() {
            var parser = new LuceneQueryParser();
            var result = await parser.ParseAsync("field1.nested:value");
            var aliasMap = new FieldMap {
                { "field1.nested", "field2.nested" }
            };
            var aliased = await FieldResolverQueryVisitor.RunAsync(result, aliasMap);
            Assert.Equal("field2.nested:value", aliased.ToString());
        }

        [Fact]
        public async Task CanApplyRootLevelAliasMapOnNestedTermAsync() {
            var parser = new LuceneQueryParser();
            var result = await parser.ParseAsync("field1.nested.morenested:value");
            var aliasMap = new FieldMap {
                { "field1.nested.morenested", "field2.nested.morenested" }
            };
            var aliased = await FieldResolverQueryVisitor.RunAsync(result, aliasMap);
            Assert.Equal("field2.nested.morenested:value", aliased.ToString());
        }

        [Fact]
        public async Task AliasMapShouldWorkOnGroupsAsync() {
            var parser = new LuceneQueryParser();
            var result = await parser.ParseAsync("field1:(nested:value OR thing:yep) another:works");
            var aliasMap = new FieldMap {
                { "field1.nested", "field2.other" },
                { "field1.thing", "field2.nice" }
            };
            var aliased = await FieldResolverQueryVisitor.RunAsync(result, aliasMap);
            Assert.Equal("(field2.other:value OR field2.nice:yep) another:works", aliased.ToString());
        }

        [Fact]
        public async Task CanUseResolverAsync() {
            var parser = new LuceneQueryParser();
            var result = await parser.ParseAsync("field1.nested:value");
            var aliased = await FieldResolverQueryVisitor.RunAsync(result, f => f == "field1.nested" ? "field2.nested" : null);
            Assert.Equal("field2.nested:value", aliased.ToString());
        }
    }
}
