using System;
using System.Linq;
using System.Threading.Tasks;
using Foundatio.Parsers.LuceneQueries.Extensions;
using Foundatio.Parsers.LuceneQueries.Nodes;
using Foundatio.Parsers.LuceneQueries.Visitors;
using Foundatio.Parsers.SqlQueries.Extensions;

namespace Foundatio.Parsers.SqlQueries.Visitors;

internal sealed class SqlSyntaxValidationVisitor : ChainableQueryVisitor
{
    public override async Task VisitAsync(GroupNode node, IQueryVisitorContext context)
    {
        if (node.Boost is not null)
            context.AddValidationError("Boost is not supported by SQL queries.");
        if (node.Proximity is not null)
            context.AddValidationError("Proximity is not supported by SQL queries.");

        await base.VisitAsync(node, context).AnyContext();
    }

    public override void Visit(TermNode node, IQueryVisitorContext context)
    {
        if (node.IsRegexTerm)
            context.AddValidationError("Regex is not supported by SQL queries.");
        if (node.Proximity is not null)
            context.AddValidationError(node.IsQuotedTerm
                ? "Phrase proximity is not supported by SQL queries."
                : "Fuzzy matching is not supported by SQL queries.");
        if (node.Boost is not null)
            context.AddValidationError("Boost is not supported by SQL queries.");

        var wildcard = SqlWildcardPattern.Analyze(node);
        if (wildcard.Kind == SqlWildcardKind.None || context is not ISqlQueryVisitorContext sqlContext)
            return;

        string[] fields = String.IsNullOrEmpty(node.Field) ? context.DefaultFields ?? [] : [node.Field];
        foreach (string fieldName in fields)
        {
            var field = SqlNodeExtensions.GetFieldInfo(sqlContext.Fields, fieldName);
            if (!field.IsString)
                context.AddValidationError($"Wildcard patterns require a mapped string field: {fieldName}.");
            else if (wildcard.Kind == SqlWildcardKind.Advanced
                && sqlContext.FullTextFields?.Contains(field.FullName, StringComparer.OrdinalIgnoreCase) is true)
                context.AddValidationError($"Advanced wildcard patterns are not supported on full-text fields: {fieldName}.");
        }
    }

    public override void Visit(TermRangeNode node, IQueryVisitorContext context)
    {
        if (node.Boost is not null)
            context.AddValidationError("Boost is not supported by SQL queries.");
        if (node.Proximity is not null)
            context.AddValidationError("Proximity is not supported by SQL queries.");
    }
}
