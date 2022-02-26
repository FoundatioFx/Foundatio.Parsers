using System;
using System.Collections.Generic;
using System.Threading.Tasks;
using Foundatio.Parsers.LuceneQueries.Nodes;
using Foundatio.Parsers.LuceneQueries.Visitors;
using Nest;
using Foundatio.Parsers.ElasticQueries.Extensions;

namespace Foundatio.Parsers.ElasticQueries.Visitors;

public class GetSortFieldsVisitor : QueryNodeVisitorWithResultBase<IEnumerable<IFieldSort>> {
    private readonly List<IFieldSort> _fields = new();

    public override void Visit(TermNode node, IQueryVisitorContext context) {
        if (String.IsNullOrEmpty(node.Field))
            return;

        var sort = node.GetSort(() => node.GetDefaultSort(context));
        if (sort.SortKey == null)
            return;

        _fields.Add(sort);
    }

    public override async Task<IEnumerable<IFieldSort>> AcceptAsync(IQueryNode node, IQueryVisitorContext context) {
        await node.AcceptAsync(this, context).ConfigureAwait(false);
        return _fields;
    }

    public static Task<IEnumerable<IFieldSort>> RunAsync(IQueryNode node, IQueryVisitorContext context = null) {
        return new GetSortFieldsVisitor().AcceptAsync(node, context);
    }

    public static IEnumerable<IFieldSort> Run(IQueryNode node, IQueryVisitorContext context = null) {
        return RunAsync(node, context).GetAwaiter().GetResult();
    }
}
