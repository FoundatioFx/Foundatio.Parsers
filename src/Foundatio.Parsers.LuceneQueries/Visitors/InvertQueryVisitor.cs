using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using Foundatio.Parsers.LuceneQueries.Extensions;
using Foundatio.Parsers.LuceneQueries.Nodes;

namespace Foundatio.Parsers.LuceneQueries.Visitors;

public class InvertQueryVisitor : ChainableMutatingQueryVisitor
{
    public InvertQueryVisitor(IEnumerable<string> nonInvertedFields = null)
    {
        if (nonInvertedFields != null)
            NonInvertedFields.AddRange(nonInvertedFields);
    }

    public List<string> NonInvertedFields { get; } = new List<string>();

    public override Task<IQueryNode> VisitAsync(GroupNode node, IQueryVisitorContext context)
    {
        bool onlyNonInvertedFields = node.GetReferencedFields().All(f => NonInvertedFields.Contains(f, StringComparer.OrdinalIgnoreCase));
        bool hasNonInvertedFields = node.GetReferencedFields().Any(f => NonInvertedFields.Contains(f, StringComparer.OrdinalIgnoreCase));
        bool onlyInvertedFields = !hasNonInvertedFields;

        // don't invert groups that only contain non-inverted fields
        if (onlyNonInvertedFields)
            return Task.FromResult<IQueryNode>(node);

        if (onlyInvertedFields)
        {
            // invert, don't visit children
            node = node.InvertNegation() as GroupNode;

            var alternateInvertedCriteria = context.GetAlternateInvertedCriteria();
            if (alternateInvertedCriteria != null)
                node = node.ReplaceSelf(new GroupNode
                {
                    Left = alternateInvertedCriteria,
                    Right = node.Clone(),
                    Operator = GroupOperator.Or,
                    HasParens = true
                });

            return Task.FromResult<IQueryNode>(node);
        }

        // otherwise, continue visiting children and invert them
        return base.VisitAsync(node, context);
    }

    public override Task<IQueryNode> VisitAsync(IQueryNode node, IQueryVisitorContext context)
    {
        if (context.DefaultOperator == GroupOperator.Or)
            throw new ArgumentException("Queries using OR as the default operator can not be inverted.");

        if (node is GroupNode groupNode)
            return VisitAsync(groupNode, context);

        if (node is IFieldQueryNode fieldNode && NonInvertedFields.Contains(fieldNode.Field, StringComparer.OrdinalIgnoreCase))
            return Task.FromResult(node);

        node = node.InvertNegation();

        var alternateInvertedCriteria = context.GetAlternateInvertedCriteria();
        if (alternateInvertedCriteria != null)
            node = node.ReplaceSelf(new GroupNode
            {
                Left = alternateInvertedCriteria,
                Right = node.Clone(),
                Operator = GroupOperator.Or,
                HasParens = true
            });

        return Task.FromResult(node);
    }

    public override Task<IQueryNode> AcceptAsync(IQueryNode node, IQueryVisitorContext context)
    {
        return node.AcceptAsync(this, context);
    }

    public static async Task<string> RunAsync(IQueryNode node, IEnumerable<string> nonInvertedFields = null, IQueryVisitorContext context = null)
    {
        var result = await new InvertQueryVisitor(nonInvertedFields).AcceptAsync(node, context);
        return result.ToString();
    }

    public static string Run(IQueryNode node, IEnumerable<string> nonInvertedFields = null, IQueryVisitorContext context = null)
    {
        return RunAsync(node, nonInvertedFields, context).GetAwaiter().GetResult();
    }
}
