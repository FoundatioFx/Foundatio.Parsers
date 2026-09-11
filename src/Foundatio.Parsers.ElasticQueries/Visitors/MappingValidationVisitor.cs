using System;
using System.Threading.Tasks;
using Foundatio.Parsers.ElasticQueries.Extensions;
using Foundatio.Parsers.LuceneQueries;
using Foundatio.Parsers.LuceneQueries.Extensions;
using Foundatio.Parsers.LuceneQueries.Nodes;
using Foundatio.Parsers.LuceneQueries.Visitors;

namespace Foundatio.Parsers.ElasticQueries.Visitors;

internal sealed class MappingValidationVisitor : ChainableQueryVisitor
{
    public override async Task<IQueryNode?> AcceptAsync(IQueryNode node, IQueryVisitorContext context)
    {
        if (context.GetValidationOptions().AllowUnresolvedFields
            || !context.GetValidationResult().IsValid
            || context is not IElasticQueryVisitorContext elasticContext
            || ReferenceEquals(elasticContext.MappingResolver, ElasticMappingResolver.NullInstance))
        {
            return node;
        }

        using var mappingScope = context.BeginMappingScope();
        return await base.AcceptAsync(node, context).AnyContext();
    }

    public override async Task VisitAsync(GroupNode node, IQueryVisitorContext context)
    {
        await ValidateFieldsAsync(node, context).AnyContext();
        await base.VisitAsync(node, context).AnyContext();
    }

    public override Task VisitAsync(TermNode node, IQueryVisitorContext context) => ValidateFieldsAsync(node, context);

    public override Task VisitAsync(TermRangeNode node, IQueryVisitorContext context) => ValidateFieldsAsync(node, context);

    public override Task VisitAsync(ExistsNode node, IQueryVisitorContext context) => ValidateFieldsAsync(node, context);

    public override Task VisitAsync(MissingNode node, IQueryVisitorContext context) => ValidateFieldsAsync(node, context);

    private static async Task ValidateFieldsAsync(IFieldQueryNode node, IQueryVisitorContext context)
    {
        if (!String.IsNullOrEmpty(node.Field))
        {
            await ValidateFieldAsync(node.Field, node.GetOriginalField() ?? node.Field, context).AnyContext();
        }
        else if (node is not GroupNode && context.QueryType is QueryTypes.Query
            && node.GetDefaultFields(context.DefaultFields) is { } defaultFields)
        {
            foreach (string field in defaultFields)
                await ValidateFieldAsync(field, field, context).AnyContext();
        }
    }

    private static async Task ValidateFieldAsync(string field, string originalField, IQueryVisitorContext context)
    {
        if (String.IsNullOrEmpty(field) || (context.QueryType is QueryTypes.Aggregation && field.StartsWith("@"))
            || context.GetValidationResult().UnresolvedFields.Contains(originalField))
            return;

        try
        {
            if (await ElasticQueryParser.ResolveMappingFieldAsync(field, context).AnyContext() is null)
                context.GetValidationResult().UnresolvedFields.Add(originalField);
        }
        catch (Exception ex)
        {
            context.AddValidationError($"Error in field resolver callback when resolving field ({originalField}): {ex.Message}");
        }
    }
}
