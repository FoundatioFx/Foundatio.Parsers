using System;
using System.Collections.Generic;
using System.Linq;
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
        if (nestedProperty == null)
            return base.VisitAsync(node, context);

        if (context.QueryType == QueryTypes.Aggregation)
            node.SetNestedPath(nestedProperty);
        else
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
        if (IsInsideNestedGroup(node, context))
            return;

        string nestedProperty = GetNestedProperty(node.Field, context);
        if (nestedProperty == null)
            return;

        if (context.QueryType == QueryTypes.Aggregation)
        {
            // For aggregations, just mark the node with its nested path
            node.SetNestedPath(nestedProperty);
        }
        else if (context.QueryType == QueryTypes.Query)
        {
            // For queries, wrap the query in a nested query
            var innerQuery = await node.GetQueryAsync(() => node.GetDefaultQueryAsync(context));
            if (innerQuery == null)
                return;

            var nestedQuery = new NestedQuery
            {
                Path = nestedProperty,
                Query = innerQuery
            };

            node.SetQuery(nestedQuery);
        }
    }

    private bool IsInsideNestedGroup(IQueryNode node, IQueryVisitorContext context)
    {
        var parent = node.Parent;
        while (parent != null)
        {
            if (parent is GroupNode groupNode && !String.IsNullOrEmpty(groupNode.Field))
            {
                string nestedProperty = GetNestedProperty(groupNode.Field, context);
                if (nestedProperty != null)
                    return true;
            }
            parent = parent.Parent;
        }
        return false;
    }

    private string GetNestedProperty(string fullName, IQueryVisitorContext context)
    {
        string[] nameParts = fullName?.Split('.').ToArray();

        if (nameParts == null || context is not IElasticQueryVisitorContext elasticContext || nameParts.Length == 0)
            return null;

        string fieldName = String.Empty;
        for (int i = 0; i < nameParts.Length; i++)
        {
            if (i > 0)
                fieldName += ".";

            fieldName += nameParts[i];

            if (elasticContext.MappingResolver.IsNestedPropertyType(fieldName))
                return fieldName;
        }

        return null;
    }
}
