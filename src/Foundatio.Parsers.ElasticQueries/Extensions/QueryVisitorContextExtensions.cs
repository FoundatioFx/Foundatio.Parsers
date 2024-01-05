using System;
using System.Threading.Tasks;
using Foundatio.Parsers.ElasticQueries.Visitors;
using Foundatio.Parsers.LuceneQueries.Extensions;
using Foundatio.Parsers.LuceneQueries.Nodes;
using Foundatio.Parsers.LuceneQueries.Visitors;
using Nest;

namespace Foundatio.Parsers.ElasticQueries.Extensions;

public static class QueryVisitorContextExtensions
{
    public static bool? IsRuntimeFieldResolverEnabled<T>(this T context) where T : IQueryVisitorContext
    {
        if (context is not IElasticQueryVisitorContext elasticContext)
            throw new ArgumentException("Context must be of type IElasticQueryVisitorContext", nameof(context));

        return elasticContext.EnableRuntimeFieldResolver;
    }

    public static RuntimeFieldResolver GetRuntimeFieldResolver(this IQueryVisitorContext context)
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

    public static Task<string> GetTimeZoneAsync(this IQueryVisitorContext context)
    {
        var elasticContext = context as IElasticQueryVisitorContext;
        if (elasticContext?.DefaultTimeZone != null)
            return elasticContext.DefaultTimeZone.Invoke();

        return Task.FromResult<string>(null);
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
