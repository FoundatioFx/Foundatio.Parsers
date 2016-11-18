using System;
using System.Threading.Tasks;
using Foundatio.Parsers.LuceneQueries.Nodes;
using Foundatio.Parsers.LuceneQueries.Extensions;
using System.Collections.Generic;

namespace Foundatio.Parsers.LuceneQueries.Visitors {
    public class IncludeVisitor: ChainableQueryVisitor {
        private readonly LuceneQueryParser _parser = new LuceneQueryParser();

        public override async Task VisitAsync(TermNode node, IQueryVisitorContext context) {
            if (node.Field != "@include")
                return;

            var includeResolver = context.GetIncludeResolver();
            if (includeResolver == null)
                return;

            string includedQuery = await includeResolver(node.Term).ConfigureAwait(false);
            if (String.IsNullOrEmpty(includedQuery))
                return;

            var result = _parser.Parse(includedQuery);
            await VisitAsync(result, context).ConfigureAwait(false);

            var parent = node.Parent as GroupNode;
            if (parent == null)
                return;

            if (parent.Left == node)
                parent.Left = result;
            else if (parent.Right == node)
                parent.Right = result;
        }

        public static Task<IQueryNode> RunAsync(GroupNode node, Func<string, Task<string>> includeResolver, IQueryVisitorContextWithIncludeResolver context = null) {
            return new IncludeVisitor().AcceptAsync(node, context ?? new QueryVisitorContextWithIncludeResolver { IncludeResolver = includeResolver });
        }

        public static IQueryNode Run(GroupNode node, Func<string, Task<string>> includeResolver, IQueryVisitorContextWithIncludeResolver context = null) {
            return RunAsync(node, includeResolver, context).GetAwaiter().GetResult();
        }

        public static IQueryNode Run(GroupNode node, Func<string, string> includeResolver, IQueryVisitorContextWithIncludeResolver context = null) {
            return RunAsync(node, name => Task.FromResult(includeResolver(name)), context).GetAwaiter().GetResult();
        }

        public static IQueryNode Run(GroupNode node, IDictionary<string, string> includes, IQueryVisitorContextWithIncludeResolver context = null) {
            return Run(node, name => includes.ContainsKey(name) ? includes[name] : null, context);
        }
    }
}