using System;
using System.Collections.Generic;
using System.Text;
using System.Threading.Tasks;
using Foundatio.Parsers.ElasticQueries;
using Foundatio.Parsers.ElasticQueries.Visitors;
using Foundatio.Parsers.LuceneQueries.Nodes;
using Foundatio.Parsers.LuceneQueries.Visitors;
using Xunit;

namespace Foundatio.Parsers.LuceneQueries.Tests
{
    public class UnterminatedQuotationMarkTest {
        private static readonly HashSet<string> FieldsSet = new HashSet<string>() {
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
            if (validationInfo.IsValid == false)
                return false;

            foreach (var fieldName in validationInfo.ReferencedFields) {
                if (!string.IsNullOrWhiteSpace(fieldName) && !FieldsSet.Contains(fieldName))
                    return false;
            }

            return true;
        }

        [InlineData("Hello world", true)]
        [InlineData("Hello (world)", true)]
        [InlineData("Hello (world", false)]
        [InlineData("Hello \"world\"", true)]
        [InlineData("Hello \"world", false)]        // <-- Failing case here, it should *not* be valid as there is an unterminated quotation mark. Hope this helps :)
        [InlineData("+Hello +world", true)]
        [InlineData("Hello + world", false)]
        [Theory]
        public async Task TestUnescapedQuotesQueryValidAsync(string query, bool validityExpectation) {

            bool isValid = await UnterminatedQuotationMarkTest.CheckQueryIsValidAsync(query);

            if (isValid != validityExpectation) {
                throw new Exception($"isValid ({isValid}) did not match validityExpectation ({validityExpectation}) for query: {query}.");
            }
        }
    }
}