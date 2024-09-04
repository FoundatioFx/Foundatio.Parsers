using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using Foundatio.Parsers.ElasticQueries.Extensions;
using Foundatio.Parsers.ElasticQueries.Visitors;
using Foundatio.Parsers.LuceneQueries;
using Foundatio.Parsers.LuceneQueries.Extensions;
using Foundatio.Parsers.LuceneQueries.Nodes;
using Foundatio.Parsers.LuceneQueries.Visitors;
using Nest;
using Pegasus.Common;

namespace Foundatio.Parsers.ElasticQueries;

public class ElasticQueryParser : LuceneQueryParser
{
    public ElasticQueryParser(Action<ElasticQueryParserConfiguration> configure = null)
    {
        var config = new ElasticQueryParserConfiguration();
        configure?.Invoke(config);
        Configuration = config;
    }

    public ElasticQueryParserConfiguration Configuration { get; }

    public override async Task<IQueryNode> ParseAsync(string query, IQueryVisitorContext context = null)
    {
        query ??= String.Empty;
        context ??= new ElasticQueryVisitorContext();

        SetupQueryVisitorContextDefaults(context);
        try
        {
            var result = await base.ParseAsync(query, context).ConfigureAwait(false);
            switch (context.QueryType)
            {
                case QueryTypes.Aggregation:
                    result = await Configuration.AggregationVisitor.AcceptAsync(result, context).ConfigureAwait(false);
                    break;
                case QueryTypes.Query:
                    result = await Configuration.QueryVisitor.AcceptAsync(result, context).ConfigureAwait(false);
                    break;
                case QueryTypes.Sort:
                    result = await Configuration.SortVisitor.AcceptAsync(result, context).ConfigureAwait(false);
                    break;
            }

            return result;
        }
        catch (FormatException ex)
        {
            var cursor = ex.Data["cursor"] as Cursor;
            context.GetValidationResult().QueryType = context.QueryType;
            context.AddValidationError(ex.Message, cursor.Column);

            return null;
        }
    }

    private void SetupQueryVisitorContextDefaults(IQueryVisitorContext context)
    {
        if (Configuration.EnableRuntimeFieldResolver.HasValue && !context.IsRuntimeFieldResolverEnabled().HasValue)
            context.EnableRuntimeFieldResolver(Configuration.EnableRuntimeFieldResolver.Value);

        if (Configuration.RuntimeFieldResolver != null && context.GetRuntimeFieldResolver() == null)
            context.SetRuntimeFieldResolver(Configuration.RuntimeFieldResolver);

        context.SetMappingResolver(Configuration.MappingResolver);

        if (!context.Data.ContainsKey("@OriginalContextResolver"))
            context.SetValue("@OriginalContextResolver", context.GetFieldResolver());

        context.SetFieldResolver(async (field, context) =>
        {
            string resolvedField = null;
            if (context.Data.TryGetValue("@OriginalContextResolver", out object data) && data is QueryFieldResolver resolver)
            {
                string contextResolvedField = await resolver(field, context).ConfigureAwait(false);
                if (contextResolvedField != null)
                    resolvedField = contextResolvedField;
            }

            if (Configuration.FieldResolver != null)
            {
                string configResolvedField = await Configuration.FieldResolver(resolvedField ?? field, context).ConfigureAwait(false);
                if (configResolvedField != null)
                    resolvedField = configResolvedField;
            }

            string mappingResolvedField = await MappingFieldResolver(resolvedField ?? field, context).ConfigureAwait(false);
            if (mappingResolvedField != null)
                resolvedField = mappingResolvedField;

            return resolvedField;
        });

        if (Configuration.ValidationOptions != null && !context.HasValidationOptions())
            context.SetValidationOptions(Configuration.ValidationOptions);

        if (context.QueryType == QueryTypes.Query)
        {
            context.SetDefaultFields(Configuration.DefaultFields);
            if (Configuration.IncludeResolver != null && context.GetIncludeResolver() == null)
                context.SetIncludeResolver(Configuration.IncludeResolver);
        }
    }

    private async Task<string> MappingFieldResolver(string field, IQueryVisitorContext context)
    {
        if (field == null)
            return null;

        if (context is not IElasticQueryVisitorContext elasticContext)
            throw new ArgumentException("Context must be of type IElasticQueryVisitorContext", nameof(context));

        // try to find Elasticsearch mapping
        // TODO: Need to test how runtime mappings defined on the server are handled
        // TODO: Mark fields resolved so that we don't try to do lookups multiple times

        if (elasticContext.MappingResolver != null)
        {
            var resolvedField = elasticContext.MappingResolver.GetMapping(field);
            if (resolvedField.Found)
                return resolvedField.FullPath;
        }

        // try to resolve from the list of runtime fields that are defined for this query
        if (elasticContext.RuntimeFields != null && elasticContext.RuntimeFields.Count > 0)
        {
            var resolvedRuntimeField = elasticContext.RuntimeFields.FirstOrDefault(f => f.Name.Equals(field, StringComparison.OrdinalIgnoreCase));
            if (resolvedRuntimeField != null)
                return resolvedRuntimeField.Name;
        }

        // try to use the runtime field resolver to dynamically discover a new runtime field and, if so, add it to the list of runtime fields
        if ((elasticContext.EnableRuntimeFieldResolver.HasValue == false || elasticContext.EnableRuntimeFieldResolver.Value) && elasticContext.RuntimeFieldResolver != null)
        {
            var newRuntimeField = await elasticContext.RuntimeFieldResolver(field);
            if (newRuntimeField != null)
            {
                elasticContext.RuntimeFields.Add(newRuntimeField);
                return newRuntimeField.Name;
            }
        }

        return null;
    }

    public async Task<QueryValidationResult> ValidateQueryAsync(string query, QueryValidationOptions options = null, IElasticQueryVisitorContext context = null)
    {
        context ??= new ElasticQueryVisitorContext();
        context.QueryType = QueryTypes.Query;
        context.SetValidationOptions(options);

        await ParseAsync(query, context).ConfigureAwait(false);

        return context.GetValidationResult();
    }

    public async Task<QueryContainer> BuildQueryAsync(string query, IElasticQueryVisitorContext context = null)
    {
        context ??= new ElasticQueryVisitorContext();
        context.QueryType = QueryTypes.Query;

        var result = await ParseAsync(query, context).ConfigureAwait(false);
        context.ThrowIfInvalid();

        return await BuildQueryAsync(result, context).ConfigureAwait(false);
    }

    public async Task<QueryContainer> BuildQueryAsync(IQueryNode query, IElasticQueryVisitorContext context = null)
    {
        context ??= new ElasticQueryVisitorContext();
        var q = await query.GetQueryAsync() ?? new MatchAllQuery();
        if (context?.UseScoring == false)
        {
            q = new BoolQuery
            {
                Filter = [q]
            };
        }

        return q;
    }

    public async Task<QueryValidationResult> ValidateAggregationsAsync(string query, QueryValidationOptions options = null, IElasticQueryVisitorContext context = null)
    {
        context ??= new ElasticQueryVisitorContext();
        context.QueryType = QueryTypes.Aggregation;
        context.SetValidationOptions(options);

        await ParseAsync(query, context).ConfigureAwait(false);

        return context.GetValidationResult();
    }

    public async Task<AggregationContainer> BuildAggregationsAsync(string aggregations, IElasticQueryVisitorContext context = null)
    {
        context ??= new ElasticQueryVisitorContext();
        context.QueryType = QueryTypes.Aggregation;

        var result = await ParseAsync(aggregations, context).ConfigureAwait(false);
        context.ThrowIfInvalid();

        return await BuildAggregationsAsync(result, context).ConfigureAwait(false);
    }

#pragma warning disable IDE0060 // Remove unused parameter
    public async Task<AggregationContainer> BuildAggregationsAsync(IQueryNode aggregations, IElasticQueryVisitorContext context = null)
    {
        if (aggregations == null)
            return null;

#pragma warning restore IDE0060 // Remove unused parameter
        return await aggregations?.GetAggregationAsync();
    }

    public async Task<QueryValidationResult> ValidateSortAsync(string query, QueryValidationOptions options = null, IElasticQueryVisitorContext context = null)
    {
        context ??= new ElasticQueryVisitorContext();
        context.QueryType = QueryTypes.Sort;
        context.SetValidationOptions(options);

        await ParseAsync(query, context).ConfigureAwait(false);

        return context.GetValidationResult();
    }

    public async Task<IEnumerable<IFieldSort>> BuildSortAsync(string sort, IElasticQueryVisitorContext context = null)
    {
        context ??= new ElasticQueryVisitorContext();
        context.QueryType = QueryTypes.Sort;

        var result = await ParseAsync(sort, context).ConfigureAwait(false);
        context.ThrowIfInvalid();

        return await BuildSortAsync(result, context).ConfigureAwait(false);
    }

    public Task<IEnumerable<IFieldSort>> BuildSortAsync(IQueryNode sort, IElasticQueryVisitorContext context = null)
    {
        context ??= new ElasticQueryVisitorContext();
        return GetSortFieldsVisitor.RunAsync(sort, context);
    }

    // TODO: want to be able to support things like date macro expansion (now-1d/d), geo query string filters, etc
    // date:"last 30 days"
    // number ranges field:1..
}
