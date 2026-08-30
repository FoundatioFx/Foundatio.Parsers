using System;
using System.Collections.Generic;
using System.Threading;
using System.Threading.Tasks;
using Elastic.Clients.Elasticsearch.QueryDsl;
using Foundatio.Parsers.ElasticQueries.Visitors;
using Foundatio.Parsers.LuceneQueries.Extensions;
using Foundatio.Parsers.LuceneQueries.Nodes;
using Foundatio.Parsers.LuceneQueries.Visitors;

namespace Foundatio.Parsers.ElasticQueries.Extensions;

public static class QueryVisitorContextExtensions
{
    private const string MappingResultsKey = "@MappingResults";

    internal static void ResetMappingResults(this IQueryVisitorContext context)
    {
        context.Data[MappingResultsKey] = new Dictionary<string, MappingResult>(StringComparer.OrdinalIgnoreCase);
    }

    internal static void SetMappingResult(this IQueryVisitorContext context, string requestedField, FieldMapping? mapping,
        string? resolvedField = null)
    {
        if (mapping is null)
            return;

        var mappings = GetMappingResults(context);
        mappings[requestedField] = new MappingResult(mapping, resolvedField ?? mapping.FullPath);
        mappings[mapping.FullPath] = new MappingResult(mapping, mapping.FullPath);
        if (!String.IsNullOrEmpty(resolvedField))
            mappings[resolvedField] = new MappingResult(mapping, resolvedField);
    }

    internal static bool TryGetMappingResult(this IQueryVisitorContext context, string? field, out FieldMapping? mapping)
    {
        mapping = null;
        if (String.IsNullOrEmpty(field) || !GetMappingResults(context).TryGetValue(field, out var result))
            return false;

        mapping = result.Mapping;
        return true;
    }

    internal static bool TryGetResolvedMappingField(this IQueryVisitorContext context, string field, out string? resolvedField)
    {
        if (GetMappingResults(context).TryGetValue(field, out var result))
        {
            resolvedField = result.ResolvedField;
            return true;
        }

        resolvedField = null;
        return false;
    }

    internal static FieldMapping? GetMappingResult(this IQueryVisitorContext context, string? field)
    {
        if (String.IsNullOrEmpty(field))
            return null;

        if (context.TryGetMappingResult(field, out var mapping))
            return mapping;

        mapping = context.GetMappingResolver().GetMapping(field, followAlias: true);
        context.SetMappingResult(field, mapping);
        return mapping;
    }

    internal static async ValueTask<FieldMapping?> GetMappingResultAsync(this IQueryVisitorContext context, string? field,
        CancellationToken cancellationToken = default)
    {
        if (String.IsNullOrEmpty(field))
            return null;

        if (context.TryGetMappingResult(field, out var mapping))
            return mapping;

        mapping = await context.GetMappingResolver().GetMappingAsync(field, followAlias: true, cancellationToken).AnyContext();
        context.SetMappingResult(field, mapping);
        return mapping;
    }

    private static Dictionary<string, MappingResult> GetMappingResults(IQueryVisitorContext context)
    {
        if (context.Data.TryGetValue(MappingResultsKey, out object? value)
            && value is Dictionary<string, MappingResult> mappings)
        {
            return mappings;
        }

        mappings = new Dictionary<string, MappingResult>(StringComparer.OrdinalIgnoreCase);
        context.Data[MappingResultsKey] = mappings;
        return mappings;
    }

    private readonly record struct MappingResult(FieldMapping Mapping, string ResolvedField);

    public static bool? IsRuntimeFieldResolverEnabled<T>(this T context) where T : IQueryVisitorContext
    {
        if (context is not IElasticQueryVisitorContext elasticContext)
            throw new ArgumentException("Context must be of type IElasticQueryVisitorContext", nameof(context));

        return elasticContext.EnableRuntimeFieldResolver;
    }

    public static RuntimeFieldResolver? GetRuntimeFieldResolver(this IQueryVisitorContext context)
    {
        if (context is not IElasticQueryVisitorContext elasticContext)
            throw new ArgumentException("Context must be of type IElasticQueryVisitorContext", nameof(context));

        return elasticContext.RuntimeFieldResolver;
    }

    public static T EnableRuntimeFieldResolver<T>(this T context, bool enabled = true) where T : IQueryVisitorContext
    {
        if (context is not IElasticQueryVisitorContext elasticContext)
            throw new ArgumentException("Context must be of type IElasticQueryVisitorContext", nameof(context));

        elasticContext.EnableRuntimeFieldResolver = enabled;

        return context;
    }

    public static T SetRuntimeFieldResolver<T>(this T context, RuntimeFieldResolver resolver) where T : IQueryVisitorContext
    {
        if (context is not IElasticQueryVisitorContext elasticContext)
            throw new ArgumentException("Context must be of type IElasticQueryVisitorContext", nameof(context));

        elasticContext.RuntimeFieldResolver = resolver;

        return context;
    }

    public static Task<string?> GetTimeZoneAsync(this IQueryVisitorContext context)
    {
        var elasticContext = context as IElasticQueryVisitorContext;
        if (elasticContext?.DefaultTimeZone != null)
            return elasticContext.DefaultTimeZone.Invoke()!;

        return Task.FromResult<string?>(null);
    }

    public static T SetMappingResolver<T>(this T context, ElasticMappingResolver mappingResolver) where T : IQueryVisitorContext
    {
        if (context is not IElasticQueryVisitorContext elasticContext)
            throw new ArgumentException("Context must be of type IElasticQueryVisitorContext", nameof(context));

        elasticContext.MappingResolver = mappingResolver ?? ElasticMappingResolver.NullInstance;

        return context;
    }

    public static ElasticMappingResolver GetMappingResolver<T>(this T context) where T : IQueryVisitorContext
    {
        if (context is not IElasticQueryVisitorContext elasticContext)
            throw new ArgumentException("Context must be of type IElasticQueryVisitorContext", nameof(context));

        return elasticContext.MappingResolver ?? ElasticMappingResolver.NullInstance;
    }

    public static T UseSearchMode<T>(this T context) where T : IQueryVisitorContext
    {
        context.SetDefaultOperator(GroupOperator.Or);
        context.UseScoring();

        return context;
    }

    public static T SetDefaultOperator<T>(this T context, Operator defaultOperator) where T : IQueryVisitorContext
    {
        if (defaultOperator == Operator.And)
            context.DefaultOperator = GroupOperator.And;
        else if (defaultOperator == Operator.Or)
            context.DefaultOperator = GroupOperator.Or;

        return context;
    }

    public static T UseScoring<T>(this T context) where T : IQueryVisitorContext
    {
        if (context is not IElasticQueryVisitorContext elasticContext)
            throw new ArgumentException("Context must be of type IElasticQueryVisitorContext", nameof(context));

        elasticContext.UseScoring = true;

        return context;
    }
}
