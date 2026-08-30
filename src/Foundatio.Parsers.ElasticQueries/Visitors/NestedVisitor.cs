using System;
using System.Threading.Tasks;
using Elastic.Clients.Elasticsearch.QueryDsl;
using Foundatio.Parsers.ElasticQueries.Extensions;
using Foundatio.Parsers.LuceneQueries;
using Foundatio.Parsers.LuceneQueries.Extensions;
using Foundatio.Parsers.LuceneQueries.Nodes;
using Foundatio.Parsers.LuceneQueries.Visitors;

namespace Foundatio.Parsers.ElasticQueries.Visitors;

public class NestedVisitor : ChainableQueryVisitor
{
    private readonly NestedFilterResolver? _filterResolver;

    public NestedVisitor(NestedFilterResolver? filterResolver = null)
    {
        _filterResolver = filterResolver;
    }

    public override async Task VisitAsync(GroupNode node, IQueryVisitorContext context)
    {
        if (String.IsNullOrEmpty(node.Field))
        {
            await base.VisitAsync(node, context).AnyContext();
            return;
        }

        string? nestedProperty = await GetNestedPropertyAsync(node.Field, context).AnyContext();
        if (nestedProperty is null)
        {
            await base.VisitAsync(node, context).AnyContext();
            return;
        }

        node.SetNestedPath(nestedProperty);
        if (context.QueryType is not QueryTypes.Aggregation and not QueryTypes.Sort)
            node.SetQuery(new NestedQuery { Path = nestedProperty, Query = new MatchAllQuery() });

        if (_filterResolver is not null)
        {
            await VisitGroupWithFilterAsync(node, nestedProperty, context).AnyContext();
            return;
        }

        await base.VisitAsync(node, context).AnyContext();
    }

    private async Task VisitGroupWithFilterAsync(GroupNode node, string nestedProperty, IQueryVisitorContext context)
    {
        ArgumentException.ThrowIfNullOrEmpty(nestedProperty);

        if (node.Field is not { } field || _filterResolver is null)
        {
            await base.VisitAsync(node, context).AnyContext();
            return;
        }

        string? originalField = node.GetOriginalField();
        var filter = await _filterResolver(nestedProperty, originalField ?? field, field, context).AnyContext();
        if (filter is not null)
            node.SetNestedFilter(filter);

        await base.VisitAsync(node, context).AnyContext();
    }

    public override Task VisitAsync(TermNode node, IQueryVisitorContext context)
    {
        return HandleNestedFieldNodeAsync(node, context);
    }

    public override Task VisitAsync(TermRangeNode node, IQueryVisitorContext context)
    {
        return HandleNestedFieldNodeAsync(node, context);
    }

    public override Task VisitAsync(ExistsNode node, IQueryVisitorContext context)
    {
        return HandleNestedFieldNodeAsync(node, context);
    }

    public override Task VisitAsync(MissingNode node, IQueryVisitorContext context)
    {
        return HandleNestedFieldNodeAsync(node, context);
    }

    private async Task HandleNestedFieldNodeAsync(IFieldQueryNode node, IQueryVisitorContext context)
    {
        string? nestedProperty = await GetNestedPropertyAsync(node.Field, context).AnyContext();
        if (nestedProperty is null)
            return;

        if (IsInsideMatchingNestedGroup(node, nestedProperty))
            return;

        if (_filterResolver is not null)
        {
            await HandleNestedFieldWithFilterAsync(node, nestedProperty, context).AnyContext();
            return;
        }

        if (context.QueryType is QueryTypes.Aggregation or QueryTypes.Sort)
        {
            node.SetNestedPath(nestedProperty);
            return;
        }

        if (context.QueryType is QueryTypes.Query)
            await WrapInNestedQueryAsync(node, nestedProperty, context).AnyContext();
    }

    private async Task HandleNestedFieldWithFilterAsync(IFieldQueryNode node, string nestedProperty, IQueryVisitorContext context)
    {
        ArgumentException.ThrowIfNullOrEmpty(nestedProperty);

        if (node.Field is not { } field || _filterResolver is null)
            return;

        string? originalField = node.GetOriginalField();
        var filter = await _filterResolver(nestedProperty, originalField ?? field, field, context).AnyContext();
        if (filter is not null)
            node.SetNestedFilter(filter);

        if (context.QueryType is QueryTypes.Aggregation or QueryTypes.Sort)
        {
            node.SetNestedPath(nestedProperty);
        }
        else if (context.QueryType is QueryTypes.Query)
        {
            var innerQuery = await node.GetQueryAsync(() => node.GetDefaultQueryAsync(context)).AnyContext();
            if (innerQuery is null)
                return;

            node.SetQuery(new NestedQuery { Path = nestedProperty, Query = innerQuery });
        }
    }

    private static async Task WrapInNestedQueryAsync(IFieldQueryNode node, string nestedProperty, IQueryVisitorContext context)
    {
        var innerQuery = await node.GetQueryAsync(() => node.GetDefaultQueryAsync(context)).AnyContext();
        if (innerQuery is null)
            return;

        node.SetQuery(new NestedQuery { Path = nestedProperty, Query = innerQuery });
    }

    private static bool IsInsideMatchingNestedGroup(IQueryNode node, string nestedProperty)
    {
        var parent = node.Parent;
        while (parent is not null)
        {
            if (parent is GroupNode groupNode)
            {
                string? groupNestedPath = groupNode.GetNestedPath();
                if (groupNestedPath is not null && groupNestedPath == nestedProperty)
                    return true;
            }

            parent = parent.Parent;
        }

        return false;
    }

    private static async ValueTask<string?> GetNestedPropertyAsync(string? fullName, IQueryVisitorContext context)
    {
        if (fullName is null || context is not IElasticQueryVisitorContext)
            return null;

        await context.GetMappingResultAsync(fullName).AnyContext();
        return NestedPathResolver.GetDeepestNestedPath(fullName, context);
    }
}
