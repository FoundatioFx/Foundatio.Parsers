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
    private readonly bool _resolveMappingsAsync;

    public GetSortFieldsVisitor() : this(resolveMappingsAsync: true)
    {
    }

    private GetSortFieldsVisitor(bool resolveMappingsAsync)
    {
        _resolveMappingsAsync = resolveMappingsAsync;
    }

    public override void Visit(TermNode node, IQueryVisitorContext context)
    {
        if (String.IsNullOrEmpty(node.Field))
            return;

        var sort = node.GetSort(() => node.GetDefaultSort(context));
        if (sort is null)
            return;

        // Check if the sort has a valid Field property set (discriminated union)
        if (sort.Field is null && sort.GeoDistance is null && sort.Score is null && sort.Script is null)
            return;

        _fields.Add(sort);
    }

    public override async Task VisitAsync(TermNode node, IQueryVisitorContext context)
    {
        using var mappingScope = context.BeginMappingScope();
        if (_resolveMappingsAsync && context is IElasticQueryVisitorContext
            && !String.IsNullOrEmpty(node.Field) && node.GetSort() is null)
            await node.PrepareSortMappingAsync(context).AnyContext();

        Visit(node, context);
    }

    public override async Task<ICollection<SortOptions>> AcceptAsync(IQueryNode node, IQueryVisitorContext context)
    {
        using var mappingScope = context.BeginMappingScope();
        await node.AcceptAsync(this, context).AnyContext();
        return _fields;
    }

    public static Task<ICollection<SortOptions>> RunAsync(IQueryNode node, IQueryVisitorContext? context = null)
    {
        context ??= new QueryVisitorContext();
        return new GetSortFieldsVisitor().AcceptAsync(node, context);
    }

    public static ICollection<SortOptions> Run(IQueryNode node, IQueryVisitorContext? context = null)
    {
        context ??= new QueryVisitorContext();
        return new GetSortFieldsVisitor(resolveMappingsAsync: false).AcceptAsync(node, context).GetAwaiter().GetResult();
    }
}
