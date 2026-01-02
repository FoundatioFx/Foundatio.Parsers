using System;
using System.Collections.Generic;
using System.Threading.Tasks;
using Elastic.Clients.Elasticsearch;
using Foundatio.Parsers.ElasticQueries.Extensions;
using Foundatio.Parsers.LuceneQueries.Nodes;
using Foundatio.Parsers.LuceneQueries.Visitors;

namespace Foundatio.Parsers.ElasticQueries.Visitors;

public class GetSortFieldsVisitor : QueryNodeVisitorWithResultBase<ICollection<SortOptions>>
{
    private readonly List<SortOptions> _fields = new();

    public override void Visit(TermNode node, IQueryVisitorContext context)
    {
        if (String.IsNullOrEmpty(node.Field))
            return;

        var sort = node.GetSort(() => node.GetDefaultSort(context));
        if (sort == null)
            return;

        // Check if the sort has a valid Field property set (discriminated union)
        if (sort.Field == null && sort.GeoDistance == null && sort.Score == null && sort.Script == null)
            return;

        _fields.Add(sort);
    }

    public override async Task<ICollection<SortOptions>> AcceptAsync(IQueryNode node, IQueryVisitorContext context)
    {
        await node.AcceptAsync(this, context).ConfigureAwait(false);
        return _fields;
    }

    public static Task<ICollection<SortOptions>> RunAsync(IQueryNode node, IQueryVisitorContext context = null)
    {
        return new GetSortFieldsVisitor().AcceptAsync(node, context);
    }

    public static ICollection<SortOptions> Run(IQueryNode node, IQueryVisitorContext context = null)
    {
        return RunAsync(node, context).GetAwaiter().GetResult();
    }
}
