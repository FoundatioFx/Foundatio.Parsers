using System;
using System.Collections.Generic;
using System.Text;
using System.Threading.Tasks;
using Foundatio.Parsers.ElasticQueries;
using Foundatio.Parsers.ElasticQueries.Visitors;
using Foundatio.Parsers.LuceneQueries.Nodes;
using Foundatio.Parsers.LuceneQueries.Visitors;
using Xunit;

namespace Foundatio.Parsers.LuceneQueries.Tests {
    public class QueryValidationTests {
        private static readonly HashSet<string> FieldsSet = new HashSet<string>()
        {
            // Standard Fields
            "id",
            "parentId",
            "ancestorIds",
            "libraryId",
            "typeId",
            "name",
            "description",
            "text",
            "type",
            "mediaType",
            "active",
            "publishDate",
            "expiryDate",
            "createdDate",
            "modifiedDate",
            "tags",
        };

        private static ElasticQueryVisitorContext Context = new ElasticQueryVisitorContext { QueryType = QueryType.Query, DefaultOperator = GroupOperator.Default };

        /// <summary>
        /// Check if a given query is valid.
        /// </summary>
        private async static Task<bool> CheckQueryIsValidAsync(string? query) {
            ElasticQueryParser parser = new ElasticQueryParser(conf =>
                conf.UseValidation(info => Task.FromResult(ValidateQueryInfo(info)))
            );

            try {
                IQueryNode queryNode = await parser.ParseAsync(query, Context);
                return true;
            } catch (Exception ex) {
                return false;
            }
        }

        private static bool ValidateQueryInfo(QueryValidationInfo validationInfo) {
            if (validationInfo.IsValid == false) {
                return false;
            }

            foreach (var fieldName in validationInfo.ReferencedFields) {
                if (!string.IsNullOrWhiteSpace(fieldName) && !FieldsSet.Contains(fieldName)) {
                    return false;
                }
            }

            return true;
        }

        /// <summary>
        /// These values are expected to be found as invalid by the parser, but they are not, the parser returns valid.
        /// But when they are sent off to ES they throw 500 errors, so it is expected that something is wrong with the parser.
        /// </summary>
        [InlineData("/abc", false)]     // A Regex sequence is started and left unterminated.
        [InlineData("xy/z", false)]     // Same as above.
        [InlineData("quik~2c", false)]  // Not sure what is wrong here, but if you put a space after the 2 then it works, probably proximity operator.
        [InlineData("ab~2z", false)]    // Same as above.
        [Theory]
        public async Task TestUnescapedQuotesQueryValidAsync(string query, bool validityExpectation) {
            bool isValid = await QueryValidationTests.CheckQueryIsValidAsync(query);

            if (isValid != validityExpectation) {
                throw new Exception($"isValid ({isValid}) did not match validityExpectation ({validityExpectation}) for query: {query}.");
            }
        }
    }
}