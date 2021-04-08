using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using Foundatio.Parsers.LuceneQueries.Extensions;
using Foundatio.Parsers.LuceneQueries.Nodes;

namespace Foundatio.Parsers.LuceneQueries.Visitors {
    public class InvertQueryVisitor : ChainableMutatingQueryVisitor {
        public InvertQueryVisitor(IEnumerable<string> nonInvertedFields = null) {
            NonInvertedFields.AddRange(nonInvertedFields);
        }

        public List<string> NonInvertedFields { get; } = new List<string>();

        public override Task<IQueryNode> VisitAsync(GroupNode node, IQueryVisitorContext context) {
            if (node.HasParens) {
                // don't call base inside here to make sure that we don't visit nodes past the first set of parens

                // don't invert groups that only contain inverted fields
                if (node.GetReferencedFields().All(f => NonInvertedFields.Contains(f, StringComparer.OrdinalIgnoreCase)))
                    return Task.FromResult<IQueryNode>(node);

                // invert the group
                if (node.IsNegated.HasValue)
                    node.IsNegated = node.IsNegated.HasValue ? !node.IsNegated.Value : true;
                else
                    node.IsNegated = true;

                return Task.FromResult<IQueryNode>(node);
            } else {
                // check to see if we reference any non-inverted fields at the current grouping level
                // if there aren't any non-inverted fields, then just group and invert the entire thing
                if (node.GetReferencedFields().All(f => !NonInvertedFields.Contains(f, StringComparer.OrdinalIgnoreCase))) {
                    if (node.Left is GroupNode || node.Right is GroupNode)
                        node.HasParens = true;

                    if (node.Right == null && node.Left is IFieldQueryNode leftField && leftField.IsNegated()) {
                        leftField.IsNegated = false;
                    } else if (node.Left == null && node.Right is IFieldQueryNode rightField && rightField.IsNegated()) {
                        rightField.IsNegated = false;
                    } else {
                        node.IsNegated = node.IsNegated.HasValue ? !node.IsNegated.Value : true;
                    }

                    return Task.FromResult<IQueryNode>(node);
                }
            }

            return base.VisitAsync(node, context);
        }

        public override Task<IQueryNode> VisitAsync(IQueryNode node, IQueryVisitorContext context) {
            if (node is GroupNode groupNode)
                return VisitAsync(groupNode, context);

            if (node is IFieldQueryNode fieldNode && NonInvertedFields.Contains(fieldNode.Field, StringComparer.OrdinalIgnoreCase))
                return Task.FromResult(node);

            return Task.FromResult(node.ReplaceSelf(new GroupNode {
                Left = node.Clone(),
                HasParens = true,
                IsNegated = true
            }));
        }

        public override Task<IQueryNode> AcceptAsync(IQueryNode node, IQueryVisitorContext context) {
            return node.AcceptAsync(this, context);
        }

        public static async Task<string> RunAsync(IQueryNode node, IEnumerable<string> nonInvertedFields = null, IQueryVisitorContext context = null) {
            var result = await new InvertQueryVisitor(nonInvertedFields).AcceptAsync(node, context);
            return result.ToString();
        }

        public static string Run(IQueryNode node, IEnumerable<string> nonInvertedFields = null, IQueryVisitorContext context = null) {
            return RunAsync(node, nonInvertedFields, context).GetAwaiter().GetResult();
        }
    }
}
