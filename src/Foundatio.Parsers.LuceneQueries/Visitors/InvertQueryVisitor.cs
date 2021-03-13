using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using Foundatio.Parsers.LuceneQueries.Extensions;
using Foundatio.Parsers.LuceneQueries.Nodes;

namespace Foundatio.Parsers.LuceneQueries.Visitors {
    public class InvertQueryVisitor : ChainableQueryVisitor {
        public InvertQueryVisitor(IEnumerable<string> nonInvertedFields = null) {
            NonInvertedFields.AddRange(nonInvertedFields);
        }

        public List<string> NonInvertedFields { get; } = new List<string>();

        public override Task VisitAsync(GroupNode node, IQueryVisitorContext context) {
            if (node.HasParens) {
                // invert the group
                if (node.IsNegated.HasValue)
                    node.IsNegated = !node.IsNegated;
                else
                    node.IsNegated = true;

                return Task.CompletedTask;
            } else {
                // check to see if we reference any non-inverted fields at the current grouping level
                // if there aren't any non-inverted fields, then just group and invert the entire thing
                if (node.GetReferencedFields(currentGroupOnly: true).All(f => !NonInvertedFields.Contains(f, StringComparer.OrdinalIgnoreCase))) {
                    node.IsNegated = true;
                    node.HasParens = true;

                    return Task.CompletedTask;
                }
            }

            return base.VisitAsync(node, context);
        }

        public override Task VisitAsync(IQueryNode node, IQueryVisitorContext context) {
            if (node is GroupNode groupNode)
                return VisitAsync(groupNode, context);

            if (node is IFieldQueryNode fieldNode && NonInvertedFields.Contains(fieldNode.Field, StringComparer.OrdinalIgnoreCase))
                return Task.CompletedTask;

            node.ReplaceSelf(new GroupNode {
                Left = node.Clone(),
                HasParens = true,
                IsNegated = true
            });

            return Task.CompletedTask;
        }

        public override async Task<IQueryNode> AcceptAsync(IQueryNode node, IQueryVisitorContext context) {
            await node.AcceptAsync(this, context);
            return node;
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
