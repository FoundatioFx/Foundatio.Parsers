using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using Foundatio.Parsers.LuceneQueries.Extensions;
using Foundatio.Parsers.LuceneQueries.Nodes;

namespace Foundatio.Parsers.LuceneQueries.Visitors {
    public class RemoveFieldsQueryVisitor : ChainableQueryVisitor {
        public RemoveFieldsQueryVisitor(IEnumerable<string> fieldsToRemove) {
            if (fieldsToRemove == null)
                throw new ArgumentNullException(nameof(fieldsToRemove));

            var fieldsToRemoveList = fieldsToRemove.ToArray();
            ShouldRemoveField = f => fieldsToRemoveList.Contains(f, StringComparer.OrdinalIgnoreCase);
        }

        public RemoveFieldsQueryVisitor(Func<string, bool> shouldRemoveFieldFunc) {
            if (shouldRemoveFieldFunc == null)
                throw new ArgumentNullException(nameof(shouldRemoveFieldFunc));

            ShouldRemoveField = shouldRemoveFieldFunc;
        }

        public Func<string, bool> ShouldRemoveField { get; }

        public override Task VisitAsync(IQueryNode node, IQueryVisitorContext context) {
            if (node is IFieldQueryNode fieldNode && fieldNode.Field != null && ShouldRemoveField(fieldNode.Field)) {
                node.RemoveSelf();
                return Task.CompletedTask;
            }

            return base.VisitAsync(node, context);
        }

        public override async Task<IQueryNode> AcceptAsync(IQueryNode node, IQueryVisitorContext context) {
            await node.AcceptAsync(this, context);
            return node;
        }

        public static async Task<string> RunAsync(IQueryNode node, IEnumerable<string> nonInvertedFields = null, IQueryVisitorContext context = null) {
            var result = await new RemoveFieldsQueryVisitor(nonInvertedFields).AcceptAsync(node, context);
            return result.ToString();
        }

        public static async Task<string> RunAsync(IQueryNode node, Func<string, bool> shouldRemoveFieldFunc, IQueryVisitorContext context = null) {
            var result = await new RemoveFieldsQueryVisitor(shouldRemoveFieldFunc).AcceptAsync(node, context);
            return result.ToString();
        }

        public static string Run(IQueryNode node, IEnumerable<string> nonInvertedFields = null, IQueryVisitorContext context = null) {
            return RunAsync(node, nonInvertedFields, context).GetAwaiter().GetResult();
        }

        public static string Run(IQueryNode node, Func<string, bool> shouldRemoveFieldFunc, IQueryVisitorContext context = null) {
            return RunAsync(node, shouldRemoveFieldFunc, context).GetAwaiter().GetResult();
        }
    }
}
