using System;
using System.Collections.Generic;
using System.Threading.Tasks;
using Foundatio.Parsers.LuceneQueries.Extensions;
using Foundatio.Parsers.LuceneQueries.Nodes;

namespace Foundatio.Parsers.LuceneQueries.Visitors;

[Obsolete("Use QueryNodeExtensions.GetReferencedFields() extension method instead.")]
public class GetReferencedFieldsQueryVisitor : QueryNodeVisitorWithResultBase<ISet<string>> {
    public override Task<ISet<string>> AcceptAsync(IQueryNode node, IQueryVisitorContext context) {
        return Task.FromResult(node.GetReferencedFields(context));
    }

    public static Task<ISet<string>> RunAsync(IQueryNode node, IQueryVisitorContext context = null) {
        return new GetReferencedFieldsQueryVisitor().AcceptAsync(node, context);
    }

    public static ISet<string> Run(IQueryNode node, IQueryVisitorContext context = null) {
        return RunAsync(node, context).GetAwaiter().GetResult();
    }
}
