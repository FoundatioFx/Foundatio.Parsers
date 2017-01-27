using System;
using System.Threading.Tasks;
using Foundatio.Parsers.LuceneQueries.Nodes;

namespace Foundatio.Parsers.LuceneQueries.Visitors {
    public class DynamicFieldQueryVisitor : ChainableQueryVisitor {
        private readonly Func<IFieldQueryNode, IQueryVisitorContext, Task> _func;

        public DynamicFieldQueryVisitor(Func<IFieldQueryNode, IQueryVisitorContext, Task> func) {
            _func = func;
        }

        public override async Task VisitAsync(GroupNode node, IQueryVisitorContext context) {
            await ApplyFunc(node, context).ConfigureAwait(false);
            await base.VisitAsync(node, context).ConfigureAwait(false);
        }

        public override Task VisitAsync(TermNode node, IQueryVisitorContext context) {
            return ApplyFunc(node, context);
        }

        public override Task VisitAsync(TermRangeNode node, IQueryVisitorContext context) {
            return ApplyFunc(node, context);
        }

        public override Task VisitAsync(ExistsNode node, IQueryVisitorContext context) {
            return ApplyFunc(node, context);
        }

        public override Task VisitAsync(MissingNode node, IQueryVisitorContext context) {
            return ApplyFunc(node, context);
        }

        private Task ApplyFunc(IFieldQueryNode node, IQueryVisitorContext context) {
            if (node.Parent == null)
                return Task.CompletedTask;

            return _func(node, context);
        }

        public static Task<IQueryNode> RunAsync(IQueryNode node, Func<IFieldQueryNode, IQueryVisitorContext, Task> resolver, IQueryVisitorContextWithAliasResolver context = null) {
            return new DynamicFieldQueryVisitor(resolver).AcceptAsync(node, context ?? new QueryVisitorContextWithAliasResolver());
        }

        public static IQueryNode Run(IQueryNode node, Func<IFieldQueryNode, IQueryVisitorContext, Task> resolver, IQueryVisitorContextWithAliasResolver context = null) {
            return RunAsync(node, resolver, context).GetAwaiter().GetResult();
        }
    }
}