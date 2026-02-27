using System;
using System.Text;
using System.Threading.Tasks;
using Foundatio.Parsers.ElasticQueries.Extensions;
using Foundatio.Parsers.LuceneQueries;
using Foundatio.Parsers.LuceneQueries.Nodes;
using Foundatio.Parsers.LuceneQueries.Visitors;
using Nest;

namespace Foundatio.Parsers.ElasticQueries.Visitors;

public class NestedVisitor : ChainableQueryVisitor
{
    public override Task VisitAsync(GroupNode node, IQueryVisitorContext context)
    {
        if (String.IsNullOrEmpty(node.Field))
            return base.VisitAsync(node, context);

        string nestedProperty = GetNestedProperty(node.Field, context);
        if (nestedProperty is null)
            return base.VisitAsync(node, context);

        node.SetNestedPath(nestedProperty);
        if (context.QueryType is not QueryTypes.Aggregation and not QueryTypes.Sort)
            node.SetQuery(new NestedQuery { Path = nestedProperty });

        return base.VisitAsync(node, context);
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
        // Skip if inside a group that references a nested path
        if (IsInsideNestedGroup(node))
            return;

        string nestedProperty = GetNestedProperty(node.Field, context);
        if (nestedProperty is null)
            return;

        if (context.QueryType is QueryTypes.Aggregation or QueryTypes.Sort)
        {
            node.SetNestedPath(nestedProperty);
        }
        else if (context.QueryType == QueryTypes.Query)
        {
            var innerQuery = await node.GetQueryAsync(() => node.GetDefaultQueryAsync(context));
            if (innerQuery is null)
                return;

            node.SetQuery(new NestedQuery { Path = nestedProperty, Query = innerQuery });
        }
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

    private static string GetNestedProperty(string fullName, IQueryVisitorContext context)
    {
        string[] nameParts = fullName?.Split('.');

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
