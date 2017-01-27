using System;
using System.Linq;
using System.Threading.Tasks;
using Foundatio.Parsers.LuceneQueries;
using Foundatio.Parsers.LuceneQueries.Nodes;
using Foundatio.Parsers.LuceneQueries.Visitors;
using Xunit;

namespace Foundatio.Parsers.Tests {
    public class DynamicFieldQueryVisitorTests {
        [Theory]
        [InlineData("name:blake", "name:blake")]
        [InlineData("data.name:blake", "idx.name-s:blake")]
        [InlineData("ref.name:blake", "idx.name-r:blake")]
        public async Task CanUseDynamicAliasAsync(string input, string expected) {
            var parser = new LuceneQueryParser();
            var parseResult = await parser.ParseAsync(input);
            var result = await DynamicFieldQueryVisitor.RunAsync(parseResult, (node, context) => {
                if (node.Parent == null || String.IsNullOrEmpty(node.Field))
                    return Task.CompletedTask;

                var termNode = node as TermNode;
                if (String.IsNullOrEmpty(termNode?.Term))
                    return Task.CompletedTask;

                string[] parts = node.Field.Split('.');
                if (parts.Length != 2)
                    return Task.CompletedTask;

                if (String.Equals(parts[0], "data")) {
                    string termType = GetTermType(termNode.UnescapedTerm);
                    node.Field = $"idx.{parts[1].ToLowerInvariant()}-{termType}";
                } else if (String.Equals(parts[0], "ref")) {
                    node.Field = $"idx.{parts[1].ToLowerInvariant()}-r";
                }

                return Task.CompletedTask;
            });

            Assert.Equal(expected, result.ToString());
        }

        private static string GetTermType(params string[] terms) {
            string termType = "s";

            var trimmedTerms = terms.Where(t => t != null).Distinct().ToList();
            foreach (var term in trimmedTerms) {
                if (term.StartsWith("*"))
                    continue;

                bool boolResult;
                long numberResult;
                DateTime dateResult;
                if (Boolean.TryParse(term, out boolResult))
                    termType = "b";
                else if (Int64.TryParse(term, out numberResult))
                    termType = "n";
                else if (DateTime.TryParse(term, out dateResult))
                    termType = "d";

                break;
            }

            return termType;
        }
    }
}