using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using Foundatio.Parsers.ElasticQueries.Extensions;
using Foundatio.Parsers.LuceneQueries.Extensions;
using Foundatio.Parsers.LuceneQueries.Nodes;
using Foundatio.Parsers.LuceneQueries.Visitors;
using Nest;

namespace Foundatio.Parsers.ElasticQueries.Visitors;

public class CombineQueriesVisitor : ChainableQueryVisitor
{
    public override async Task VisitAsync(GroupNode node, IQueryVisitorContext context)
    {
        await base.VisitAsync(node, context).ConfigureAwait(false);

        // Only stop on scoped group nodes (parens). Gather all child queries (including scoped groups) and then combine them.
        // Combining only happens at the scoped group level though.
        // For all non-field terms, processes default field searches, adds them to container and combines them with AND/OR operators. merging into match/multi-match queries happen in DefaultQueryNodeExtensions.
        // Merge all nested queries for the same nested field together

        if (context is not IElasticQueryVisitorContext elasticContext)
            throw new ArgumentException("Context must be of type IElasticQueryVisitorContext", nameof(context));

        var defaultQuery = await node.GetQueryAsync(() => node.GetDefaultQueryAsync(context)).ConfigureAwait(false);

        // Will accumulate combined queries for non-nested fields
        QueryBase container = null;

        // Dictionary to accumulate queries per nested path
        var nestedQueries = new Dictionary<string, NestedQuery>();

        foreach (var child in node.Children.OfType<IFieldQueryNode>())
        {
            var childQuery = await child.GetQueryAsync(() => child.GetDefaultQueryAsync(context)).ConfigureAwait(false);
            if (childQuery == null) continue;

            if (child.IsExcluded())
                childQuery = !childQuery;

            var op = node.GetOperator(elasticContext);
            if (op == GroupOperator.Or && node.IsRequired())
                op = GroupOperator.And;

            string fieldName = child.Field;

            // Check if field is nested (has a dot) - could be improved to check against valid nested paths
            if (!String.IsNullOrEmpty(fieldName))
            {
                int dotIndex = fieldName.IndexOf('.');
                if (dotIndex > 0)
                {
                    string nestedPath = fieldName.Substring(0, dotIndex);

                    // Get or create NestedQuery for this path
                    if (!nestedQueries.TryGetValue(nestedPath, out NestedQuery nestedQuery))
                    {
                        nestedQuery = new NestedQuery
                        {
                            Path = nestedPath,
                            Query = null
                        };
                        nestedQueries[nestedPath] = nestedQuery;
                    }

                    // Combine this child's query into the nested query's inner query
                    if (nestedQuery.Query == null)
                    {
                        nestedQuery.Query = childQuery;
                    }
                    else
                    {
                        if (op == GroupOperator.And)
                            nestedQuery.Query &= childQuery;
                        else if (op == GroupOperator.Or)
                            nestedQuery.Query |= childQuery;
                    }
                }
                else
                {
                    // Non-nested field queries combined here
                    if (container == null)
                    {
                        container = childQuery;
                    }
                    else
                    {
                        if (op == GroupOperator.And)
                            container &= childQuery;
                        else if (op == GroupOperator.Or)
                            container |= childQuery;
                    }
                }
            }
            else
            {
                // Handle null field case - this is for default field searches
                if (container == null)
                {
                    container = childQuery;
                }
                else
                {
                    if (op == GroupOperator.And)
                        container &= childQuery;
                    else if (op == GroupOperator.Or)
                        container |= childQuery;
                }
            }
        }

        // Combine all nestedQueries with the container (non-nested)
        QueryBase combinedNestedQueries = null;
        foreach (var nestedQuery in nestedQueries.Values)
        {
            if (combinedNestedQueries == null)
            {
                combinedNestedQueries = nestedQuery;
            }
            else
            {
                combinedNestedQueries &= nestedQuery; // Assuming AND combining nested groups; adjust if needed
            }
        }

        QueryBase finalQuery = null;
        if (combinedNestedQueries != null && container != null)
        {
            finalQuery = combinedNestedQueries & container;
        }
        else if (combinedNestedQueries != null)
        {
            finalQuery = combinedNestedQueries;
        }
        else if (container != null)
        {
            finalQuery = container;
        }
        else
        {
            // fallback to default query
            finalQuery = defaultQuery;
        }

        node.SetQuery(finalQuery);
    }

}
