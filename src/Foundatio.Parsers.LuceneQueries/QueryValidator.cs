using System;
using System.Threading.Tasks;
using Foundatio.Parsers.LuceneQueries.Extensions;
using Foundatio.Parsers.LuceneQueries.Visitors;
using Pegasus.Common;

namespace Foundatio.Parsers.LuceneQueries {
    public class QueryValidator {
        public static Task<QueryValidationInfo> ValidateQueryAsync(string query, QueryValidationOptions options = null, IQueryVisitorContextWithValidation context = null) {
            if (context == null)
                context = new QueryVisitorContext();

            context.QueryType = QueryType.Query;

            return InternalValidateAsync(query, context, options);
        }

        public static Task<QueryValidationInfo> ValidateAggregationsAsync(string aggregations, QueryValidationOptions options = null, IQueryVisitorContextWithValidation context = null) {
            if (context == null)
                context = new QueryVisitorContext();

            context.QueryType = QueryType.Aggregation;

            return InternalValidateAsync(aggregations, context, options);
        }

        public static Task<QueryValidationInfo> ValidateSortAsync(string sort, QueryValidationOptions options = null, IQueryVisitorContextWithValidation context = null) {
            if (context == null)
                context = new QueryVisitorContext();

            context.QueryType = QueryType.Sort;

            return InternalValidateAsync(sort, context, options);
        }

        private static async Task<QueryValidationInfo> InternalValidateAsync(string query, IQueryVisitorContextWithValidation context, QueryValidationOptions options = null) {
            var parser = new LuceneQueryParser();
            try {
                var node = await parser.ParseAsync(query);
                if (context == null)
                    context = new QueryVisitorContext();

                if (options != null)
                    context.SetValidationOptions(options);

                return await ValidationVisitor.RunAsync(node, context);
            } catch (FormatException ex) {
                var info = new QueryValidationInfo();
                var cursor = ex.Data["cursor"] as Cursor;
                info.MarkInvalid($"[{cursor.Line}:{cursor.Column}] {ex.Message}");
                return info;
            }
        }

        public static Task<QueryValidationInfo> ValidateQueryAndThrowAsync(string query, QueryValidationOptions options = null, IQueryVisitorContextWithValidation context = null) {
            if (context == null)
                context = new QueryVisitorContext();

            context.QueryType = QueryType.Query;

            return InternalValidateAndThrowAsync(query, context, options);
        }

        public static Task<QueryValidationInfo> ValidateAggregationsAndThrowAsync(string aggregations, QueryValidationOptions options = null, IQueryVisitorContextWithValidation context = null) {
            if (context == null)
                context = new QueryVisitorContext();

            context.QueryType = QueryType.Aggregation;

            return InternalValidateAndThrowAsync(aggregations, context, options);
        }

        public static Task<QueryValidationInfo> ValidateSortAndThrowAsync(string sort, QueryValidationOptions options = null, IQueryVisitorContextWithValidation context = null) {
            if (context == null)
                context = new QueryVisitorContext();

            context.QueryType = QueryType.Sort;

            return InternalValidateAndThrowAsync(sort, context, options);
        }

        private static async Task<QueryValidationInfo> InternalValidateAndThrowAsync(string query, IQueryVisitorContextWithValidation context, QueryValidationOptions options = null) {
            var parser = new LuceneQueryParser();
            try {
                var node = await parser.ParseAsync(query);
                if (context == null)
                    context = new QueryVisitorContext();

                options ??= new QueryValidationOptions();
                options.ShouldThrow = true;
                context.SetValidationOptions(options);

                return await ValidationVisitor.RunAsync(node, context);
            } catch (FormatException ex) {
                var info = new QueryValidationInfo();
                var cursor = ex.Data["cursor"] as Cursor;
                info.MarkInvalid($"[{cursor.Line}:{cursor.Column}] {ex.Message}");

                throw new QueryValidationException(info.Message, info, ex);
            }
        }
    }
}
