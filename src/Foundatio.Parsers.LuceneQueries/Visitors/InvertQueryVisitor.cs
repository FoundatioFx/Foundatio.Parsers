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
                // invert the group
                if (node.IsNegated.HasValue)
                    node.IsNegated = node.IsNegated.HasValue ? !node.IsNegated.Value : true;
                else
                    node.IsNegated = true;

                return Task.FromResult<IQueryNode>(node);
            } else {
                // check to see if we reference any non-inverted fields at the current grouping level
                // if there aren't any non-inverted fields, then just group and invert the entire thing
                if (node.GetReferencedFields(currentGroupOnly: true).All(f => !NonInvertedFields.Contains(f, StringComparer.OrdinalIgnoreCase))) {
                    node.IsNegated = node.IsNegated.HasValue ? !node.IsNegated.Value : true;
                    node.HasParens = true;

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
