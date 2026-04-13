using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using Elastic.Clients.Elasticsearch;
using Elastic.Clients.Elasticsearch.QueryDsl;
using Foundatio.Parsers.ElasticQueries.Extensions;
using Foundatio.Parsers.ElasticQueries.Visitors;
using Foundatio.Parsers.LuceneQueries;
using Foundatio.Parsers.LuceneQueries.Extensions;
using Foundatio.Parsers.LuceneQueries.Nodes;
using Foundatio.Parsers.LuceneQueries.Visitors;
using Pegasus.Common;

namespace Foundatio.Parsers.ElasticQueries;

public class ElasticQueryParser : LuceneQueryParser
{
    public ElasticQueryParser(Action<ElasticQueryParserConfiguration>? configure = null)
    {
        var config = new ElasticQueryParserConfiguration();
        configure?.Invoke(config);
        Configuration = config;
    }

    public ElasticQueryParserConfiguration Configuration { get; }

    public override async Task<IQueryNode?> ParseAsync(string query, IQueryVisitorContext? context = null)
    {
        query ??= String.Empty;
        context ??= new ElasticQueryVisitorContext();

        SetupQueryVisitorContextDefaults(context);
        try
        {
            var result = await base.ParseAsync(query, context).AnyContext();
            if (result is null)
                return null;

            switch (context.QueryType)
            {
                case QueryTypes.Aggregation:
                    result = await Configuration.AggregationVisitor.AcceptAsync(result, context).AnyContext();
                    break;
                case QueryTypes.Query:
                    result = await Configuration.QueryVisitor.AcceptAsync(result, context).AnyContext();
                    break;
                case QueryTypes.Sort:
                    result = await Configuration.SortVisitor.AcceptAsync(result, context).AnyContext();
                    break;
            }

            return result;
        }
        catch (FormatException ex)
        {
            var cursor = ex.Data["cursor"] as Cursor;
            context.GetValidationResult().QueryType = context.QueryType;
            context.AddValidationError(ex.Message, cursor?.Column ?? 0);

            return null;
        }
    }

    private void SetupQueryVisitorContextDefaults(IQueryVisitorContext context)
    {
        if (Configuration.EnableRuntimeFieldResolver.HasValue && !context.IsRuntimeFieldResolverEnabled().HasValue)
            context.EnableRuntimeFieldResolver(Configuration.EnableRuntimeFieldResolver.Value);

        if (Configuration.RuntimeFieldResolver is not null && context.GetRuntimeFieldResolver() is null)
            context.SetRuntimeFieldResolver(Configuration.RuntimeFieldResolver);

        context.SetMappingResolver(Configuration.MappingResolver ?? ElasticMappingResolver.NullInstance);

        if (!context.Data.ContainsKey("@OriginalContextResolver"))
            context.SetValue("@OriginalContextResolver", context.GetFieldResolver()!);

        context.SetFieldResolver(async (field, context) =>
        {
            string? resolvedField = null;
            if (context?.Data.TryGetValue("@OriginalContextResolver", out object? data) is true && data is QueryFieldResolver resolver)
            {
                string? contextResolvedField = await resolver(field, context).AnyContext();
                if (contextResolvedField is not null)
                    resolvedField = contextResolvedField;
            }

            if (Configuration.FieldResolver is not null)
            {
                string? configResolvedField = await Configuration.FieldResolver(resolvedField ?? field, context).AnyContext();
                if (configResolvedField is not null)
                    resolvedField = configResolvedField;
            }

            string? mappingResolvedField = await MappingFieldResolver(resolvedField ?? field, context).AnyContext();
            if (mappingResolvedField is not null)
                resolvedField = mappingResolvedField;

            return resolvedField;
        });

        if (Configuration.ValidationOptions is not null && !context.HasValidationOptions())
            context.SetValidationOptions(Configuration.ValidationOptions);

        if (context is IElasticQueryVisitorContext elasticVisitorContext
            && Configuration.GeoLocationResolver is not null
            && elasticVisitorContext.GeoLocationResolver is null)
        {
            elasticVisitorContext.GeoLocationResolver = Configuration.GeoLocationResolver;
        }

        if (context.QueryType == QueryTypes.Query)
        {
            if (Configuration.DefaultFields is not null)
                context.SetDefaultFields(Configuration.DefaultFields);
            if (Configuration.IncludeResolver is not null && context.GetIncludeResolver() is null)
                context.SetIncludeResolver(Configuration.IncludeResolver);
        }
    }

    private async Task<string?> MappingFieldResolver(string? field, IQueryVisitorContext? context)
    {
        if (field is null)
            return null;

        if (context is not IElasticQueryVisitorContext elasticContext)
            throw new ArgumentException("Context must be of type IElasticQueryVisitorContext", nameof(context));

        // try to find Elasticsearch mapping
        // TODO: Need to test how runtime mappings defined on the server are handled
        // TODO: Mark fields resolved so that we don't try to do lookups multiple times

        if (elasticContext.MappingResolver is not null)
        {
            var resolvedField = elasticContext.MappingResolver.GetMapping(field);
            if (resolvedField?.Found is true)
                return resolvedField.FullPath;
        }

        // try to resolve from the list of runtime fields that are defined for this query
        if (elasticContext.RuntimeFields is not null && elasticContext.RuntimeFields.Count > 0)
        {
            var resolvedRuntimeField = elasticContext.RuntimeFields.FirstOrDefault(f => f.Name.Equals(field, StringComparison.OrdinalIgnoreCase));
            if (resolvedRuntimeField is not null)
                return resolvedRuntimeField.Name;
        }

        // try to use the runtime field resolver to dynamically discover a new runtime field and, if so, add it to the list of runtime fields
        if ((elasticContext.EnableRuntimeFieldResolver.HasValue == false || elasticContext.EnableRuntimeFieldResolver.Value) && elasticContext.RuntimeFieldResolver is not null)
        {
            var newRuntimeField = await elasticContext.RuntimeFieldResolver(field).AnyContext();
            if (newRuntimeField is not null)
            {
                elasticContext.RuntimeFields?.Add(newRuntimeField);
                return newRuntimeField.Name;
            }
        }

        return null;
    }

    public async Task<QueryValidationResult> ValidateQueryAsync(string query, QueryValidationOptions? options = null, IElasticQueryVisitorContext? context = null)
    {
        context ??= new ElasticQueryVisitorContext();
        context.QueryType = QueryTypes.Query;
        if (options is not null)
            context.SetValidationOptions(options);

        await ParseAsync(query, context).AnyContext();

        return context.GetValidationResult();
    }

    public async Task<Query> BuildQueryAsync(string query, IElasticQueryVisitorContext? context = null)
    {
        context ??= new ElasticQueryVisitorContext();
        context.QueryType = QueryTypes.Query;

        var result = await ParseAsync(query, context).AnyContext();
        context.ThrowIfInvalid();

        if (result is null)
            throw new FormatException("Failed to parse query.");

        return await BuildQueryAsync(result, context).AnyContext();
    }

    public async Task<Query> BuildQueryAsync(IQueryNode query, IElasticQueryVisitorContext? context = null)
    {
        context ??= new ElasticQueryVisitorContext();
        var q = await query.GetQueryAsync().AnyContext() ?? new MatchAllQuery();
        if (!context.UseScoring)
        {
            if (!q.IsFilterOnlyBoolQuery())
            {
                q = new BoolQuery
                {
                    Filter = [q]
                };
            }
        }

        return q;
    }

    public async Task<QueryValidationResult> ValidateAggregationsAsync(string query, QueryValidationOptions? options = null, IElasticQueryVisitorContext? context = null)
    {
        context ??= new ElasticQueryVisitorContext();
        context.QueryType = QueryTypes.Aggregation;
        if (options is not null)
            context.SetValidationOptions(options);

        await ParseAsync(query, context).AnyContext();

        return context.GetValidationResult();
    }

    public async Task<AggregationMap?> BuildAggregationsAsync(string aggregations, IElasticQueryVisitorContext? context = null)
    {
        context ??= new ElasticQueryVisitorContext();
        context.QueryType = QueryTypes.Aggregation;

        var result = await ParseAsync(aggregations, context).AnyContext();
        context.ThrowIfInvalid();

        return await BuildAggregationsAsync(result, context).AnyContext();
    }

    public async Task<AggregationMap?> BuildAggregationsAsync(IQueryNode? aggregations, IElasticQueryVisitorContext? context = null)
    {
        if (aggregations is null)
            return null;

        return await aggregations.GetAggregationAsync().AnyContext();
    }

    public async Task<QueryValidationResult> ValidateSortAsync(string query, QueryValidationOptions? options = null, IElasticQueryVisitorContext? context = null)
    {
        context ??= new ElasticQueryVisitorContext();
        context.QueryType = QueryTypes.Sort;
        if (options is not null)
            context.SetValidationOptions(options);

        await ParseAsync(query, context).AnyContext();

        return context.GetValidationResult();
    }

    public async Task<ICollection<SortOptions>> BuildSortAsync(string? sort, IElasticQueryVisitorContext? context = null)
    {
        sort ??= String.Empty;
        context ??= new ElasticQueryVisitorContext();
        context.QueryType = QueryTypes.Sort;

        var result = await ParseAsync(sort, context).AnyContext();
        context.ThrowIfInvalid();

        if (result is null)
            throw new FormatException("Failed to parse sort.");

        return await BuildSortAsync(result, context).AnyContext();
    }

    public Task<ICollection<SortOptions>> BuildSortAsync(IQueryNode sort, IElasticQueryVisitorContext? context = null)
    {
        context ??= new ElasticQueryVisitorContext();
        return GetSortFieldsVisitor.RunAsync(sort, context);
    }

    // TODO: want to be able to support things like date macro expansion (now-1d/d), geo query string filters, etc
    // date:"last 30 days"
    // number ranges field:1..
}
