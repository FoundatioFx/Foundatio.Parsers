using System;
using System.Text;
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

    public override Task VisitAsync(GroupNode node, IQueryVisitorContext context)
    {
        if (String.IsNullOrEmpty(node.Field))
            return base.VisitAsync(node, context);

        string? nestedProperty = GetNestedProperty(node.Field, context);
        if (nestedProperty is null)
            return base.VisitAsync(node, context);

        node.SetNestedPath(nestedProperty);
        if (context.QueryType is not QueryTypes.Aggregation and not QueryTypes.Sort)
            node.SetQuery(new NestedQuery { Path = nestedProperty, Query = new MatchAllQuery() });

        if (_filterResolver is not null)
            return VisitGroupWithFilterAsync(node, nestedProperty, context);

        return base.VisitAsync(node, context);
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

    private Task HandleNestedFieldNodeAsync(IFieldQueryNode node, IQueryVisitorContext context)
    {
        if (IsInsideNestedGroup(node))
            return Task.CompletedTask;

        string? nestedProperty = GetNestedProperty(node.Field, context);
        if (nestedProperty is null)
            return Task.CompletedTask;

        if (_filterResolver is not null)
            return HandleNestedFieldWithFilterAsync(node, nestedProperty, context);

        if (context.QueryType is QueryTypes.Aggregation or QueryTypes.Sort)
        {
            node.SetNestedPath(nestedProperty);
            return Task.CompletedTask;
        }

        if (context.QueryType is QueryTypes.Query)
            return WrapInNestedQueryAsync(node, nestedProperty, context);

        return Task.CompletedTask;
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

    private static bool IsInsideNestedGroup(IQueryNode node)
    {
        var parent = node.Parent;
        while (parent is not null)
        {
            if (parent is GroupNode groupNode && groupNode.GetNestedPath() is not null)
                return true;

            parent = parent.Parent;
        }

        return false;
    }

    private static string? GetNestedProperty(string? fullName, IQueryVisitorContext context)
    {
        string[]? nameParts = fullName?.Split('.');

        if (nameParts is null || context is not IElasticQueryVisitorContext elasticContext || nameParts is { Length: 0 })
            return null;

        var builder = new StringBuilder();
        for (int i = 0; i < nameParts.Length; i++)
        {
            if (i > 0)
                builder.Append('.');

            builder.Append(nameParts[i]);

            string fieldName = builder.ToString();
            if (elasticContext.MappingResolver.IsNestedPropertyType(fieldName))
                return fieldName;
        }

        return null;
    }
}
