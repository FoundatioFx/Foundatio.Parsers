using System;
using System.Threading.Tasks;
using Foundatio.Parsers.LuceneQueries.Extensions;
using Foundatio.Parsers.LuceneQueries.Nodes;
using Foundatio.Parsers.LuceneQueries.Visitors;
using Pegasus.Common;

namespace Foundatio.Parsers.LuceneQueries;

public class QueryValidator
{
    public static Task<QueryValidationResult> ValidateQueryAsync(string query, QueryValidationOptions? options = null, IQueryVisitorContextWithValidation? context = null)
    {
        if (context is null)
            context = new QueryVisitorContext();

        context.QueryType = QueryTypes.Query;

        return InternalValidateAsync(query, context, options);
    }

    public static Task<QueryValidationResult> ValidateAggregationsAsync(string aggregations, QueryValidationOptions? options = null, IQueryVisitorContextWithValidation? context = null)
    {
        if (context is null)
            context = new QueryVisitorContext();

        context.QueryType = QueryTypes.Aggregation;

        return InternalValidateAsync(aggregations, context, options);
    }

    public static Task<QueryValidationResult> ValidateSortAsync(string sort, QueryValidationOptions? options = null, IQueryVisitorContextWithValidation? context = null)
    {
        if (context is null)
            context = new QueryVisitorContext();

        context.QueryType = QueryTypes.Sort;

        return InternalValidateAsync(sort, context, options);
    }

    private static async Task<QueryValidationResult> InternalValidateAsync(string query, IQueryVisitorContextWithValidation context, QueryValidationOptions? options = null)
    {
        var parser = new LuceneQueryParser();
        try
        {
            var node = await parser.ParseAsync(query).AnyContext();
            if (context is null)
                context = new QueryVisitorContext();

            if (node is null)
                return context.GetValidationResult();

            if (options is not null)
                context.SetValidationOptions(options);

            var fieldResolver = context.GetFieldResolver();
            if (fieldResolver is not null)
                node = await FieldResolverQueryVisitor.RunAsync(node, fieldResolver, context as IQueryVisitorContextWithFieldResolver) ?? node;

            var includeResolver = context.GetIncludeResolver();
            if (includeResolver is not null)
                node = await IncludeVisitor.RunAsync(node, includeResolver, context as IQueryVisitorContextWithIncludeResolver) ?? node;

            return await ValidationVisitor.RunAsync(node, context);
        }
        catch (FormatException ex)
        {
            var cursor = ex.Data["cursor"] as Cursor;
            context.AddValidationError(ex.Message, cursor?.Column ?? 0);

            return context.GetValidationResult();
        }
    }

    public static Task<QueryValidationResult> ValidateQueryAndThrowAsync(string query, QueryValidationOptions? options = null, IQueryVisitorContextWithValidation? context = null)
    {
        if (context is null)
            context = new QueryVisitorContext();

        context.QueryType = QueryTypes.Query;

        return InternalValidateAndThrowAsync(query, context, options);
    }

    public static Task<QueryValidationResult> ValidateAggregationsAndThrowAsync(string aggregations, QueryValidationOptions? options = null, IQueryVisitorContextWithValidation? context = null)
    {
        if (context is null)
            context = new QueryVisitorContext();

        context.QueryType = QueryTypes.Aggregation;

        return InternalValidateAndThrowAsync(aggregations, context, options);
    }

    public static Task<QueryValidationResult> ValidateSortAndThrowAsync(string sort, QueryValidationOptions? options = null, IQueryVisitorContextWithValidation? context = null)
    {
        if (context is null)
            context = new QueryVisitorContext();

        context.QueryType = QueryTypes.Sort;

        return InternalValidateAndThrowAsync(sort, context, options);
    }

    private static async Task<QueryValidationResult> InternalValidateAndThrowAsync(string query, IQueryVisitorContextWithValidation context, QueryValidationOptions? options = null)
    {
        var parser = new LuceneQueryParser();
        try
        {
            var node = await parser.ParseAsync(query).AnyContext();
            if (context is null)
                context = new QueryVisitorContext();

            options ??= new QueryValidationOptions();
            options.ShouldThrow = true;
            context.SetValidationOptions(options);

            // Visitors never nullify a non-null root in practice; this guard satisfies NRT
            // and produces a clear validation error if a visitor unexpectedly returns null.
            IQueryNode EnsureNode(IQueryNode? node)
            {
                if (node is not null)
                    return node;

                var validationResult = context.GetValidationResult();
                throw new QueryValidationException(validationResult.Message, validationResult);
            }

            node = EnsureNode(node);

            var fieldResolver = context.GetFieldResolver();
            if (fieldResolver != null)
                node = EnsureNode(await FieldResolverQueryVisitor.RunAsync(node, fieldResolver, context as IQueryVisitorContextWithFieldResolver));

            var includeResolver = context.GetIncludeResolver();
            if (includeResolver != null)
                node = EnsureNode(await IncludeVisitor.RunAsync(node, includeResolver, context as IQueryVisitorContextWithIncludeResolver));

            return await ValidationVisitor.RunAsync(node, context);
        }
        catch (FormatException ex)
        {
            var cursor = ex.Data["cursor"] as Cursor;
            context.AddValidationError(ex.Message, cursor?.Column ?? 0);

            throw new QueryValidationException(ex.Message, context.GetValidationResult(), ex);
        }
    }
}
