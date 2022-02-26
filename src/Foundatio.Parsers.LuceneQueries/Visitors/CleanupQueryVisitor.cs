using System;
using System.Threading.Tasks;
using Foundatio.Parsers.LuceneQueries.Extensions;
using Foundatio.Parsers.LuceneQueries.Nodes;

namespace Foundatio.Parsers.LuceneQueries.Visitors;

public class CleanupQueryVisitor : ChainableQueryVisitor {
    public override async Task<IQueryNode> VisitAsync(IQueryNode node, IQueryVisitorContext context) {
        var result = CleanNode(node);
        if (result == null)
            return null;

        result = await base.VisitAsync(result, context);

        return CleanNode(result);
    }

    private IQueryNode CleanNode(IQueryNode node) {
        if (node is GroupNode groupNode) {
            if (groupNode.Left != null && groupNode.Right != null)
                return groupNode;

            // remove non-root empty groups
            if (groupNode.Left == null && groupNode.Right == null && groupNode.Parent != null) {
                groupNode.RemoveSelf();
                return groupNode.Parent;
            }

            // don't alter groups with fields
            if (!String.IsNullOrEmpty(groupNode.Field)) {
                return groupNode;
            }

            if (groupNode.Left is GroupNode leftGroupNode && groupNode.Right == null && leftGroupNode.Field == null) {
                leftGroupNode.Field = groupNode.Field;
                groupNode.ReplaceSelf(leftGroupNode);

                // no value is set, use parent value
                if (!leftGroupNode.IsNegated.HasValue) {
                    leftGroupNode.IsNegated = groupNode.IsNegated;
                } else if (groupNode.IsNegated.HasValue) {
                    // both values set

                    // double negative
                    if (groupNode.IsNegated.Value && leftGroupNode.IsNegated.Value)
                        leftGroupNode.IsNegated = false;
                    else if (groupNode.IsNegated.Value || leftGroupNode.IsNegated.Value)
                        leftGroupNode.IsNegated = true;
                }

                groupNode = leftGroupNode;
                node = groupNode;
            }

            if (groupNode.Right is GroupNode rightGroupNode && groupNode.Left == null && rightGroupNode.Field == null) {
                rightGroupNode.Field = groupNode.Field;
                groupNode.ReplaceSelf(rightGroupNode);

                // no value is set, use parent value
                if (!rightGroupNode.IsNegated.HasValue) {
                    rightGroupNode.IsNegated = groupNode.IsNegated;
                } else if (groupNode.IsNegated.HasValue) {
                    // both values set

                    // double negative
                    if (groupNode.IsNegated.Value && rightGroupNode.IsNegated.Value)
                        rightGroupNode.IsNegated = false;
                    else if (groupNode.IsNegated.Value || rightGroupNode.IsNegated.Value)
                        rightGroupNode.IsNegated = true;
                }

                groupNode = rightGroupNode;
                node = groupNode;
            }

            // don't need parens on single term
            if (groupNode.HasParens
                && groupNode.Left is TermNode leftTermNode
                && groupNode.Right == null) {

                if (groupNode.IsNegated.HasValue && groupNode.IsNegated.Value) {
                    groupNode.HasParens = false;
                } else {
                    groupNode.ReplaceSelf(leftTermNode);
                    node = leftTermNode;
                }
            }

            // don't need parens on single term
            if (groupNode.HasParens
                && groupNode.Right is TermNode rightTermNode
                && groupNode.Left == null) {

                if (groupNode.IsNegated.HasValue && groupNode.IsNegated.Value) {
                    groupNode.HasParens = false;
                } else {
                    groupNode.ReplaceSelf(rightTermNode);
                    node = rightTermNode;
                }
            }
        }

        return node;
    }

    public static async Task<string> RunAsync(IQueryNode node, IQueryVisitorContext context = null) {
        var result = await new CleanupQueryVisitor().AcceptAsync(node, context);
        return result.ToString();
    }

    public static string Run(IQueryNode node, IQueryVisitorContext context = null) {
        return RunAsync(node, context).GetAwaiter().GetResult();
    }
}
