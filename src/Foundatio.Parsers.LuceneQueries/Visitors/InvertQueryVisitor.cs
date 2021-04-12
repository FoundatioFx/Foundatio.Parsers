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

                return Task.FromResult(node.InvertGroupNegation(context));
            } else {
                // check to see if we reference any non-inverted fields at the current grouping level
                // if there aren't any non-inverted fields, then just group and invert the entire thing
                if (node.GetReferencedFields().All(f => !NonInvertedFields.Contains(f, StringComparer.OrdinalIgnoreCase)))
                    return Task.FromResult(node.InvertGroupNegation(context));
            }

            return base.VisitAsync(node, context);
        }

        public override Task<IQueryNode> VisitAsync(IQueryNode node, IQueryVisitorContext context) {
            if (context.DefaultOperator == GroupOperator.Or)
                throw new ArgumentException("Queries using OR as the default operator can not be inverted.");

            if (node is GroupNode groupNode)
                return VisitAsync(groupNode, context);

            if (node is IFieldQueryNode fieldNode && NonInvertedFields.Contains(fieldNode.Field, StringComparer.OrdinalIgnoreCase))
                return Task.FromResult(node);

            return Task.FromResult<IQueryNode>(node.ReplaceSelf(new GroupNode {
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
