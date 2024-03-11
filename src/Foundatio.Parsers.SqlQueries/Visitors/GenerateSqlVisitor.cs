using System;
using System.Text;
using System.Threading.Tasks;
using Foundatio.Parsers.LuceneQueries.Nodes;
using Foundatio.Parsers.LuceneQueries.Visitors;

namespace Foundatio.Parsers.SqlQueries.Visitors;

public class GenerateSqlVisitor : QueryNodeVisitorWithResultBase<string>
{
    private readonly StringBuilder _builder = new();

    public override Task VisitAsync(GroupNode node, IQueryVisitorContext context)
    {
        _builder.Append(node.ToSqlString(context?.DefaultOperator ?? GroupOperator.Default));

        return Task.CompletedTask;
    }

    public override void Visit(TermNode node, IQueryVisitorContext context)
    {
        _builder.Append(node.ToSqlString());
    }

    public override void Visit(TermRangeNode node, IQueryVisitorContext context)
    {
        _builder.Append(node.ToSqlString());
    }

    public override void Visit(ExistsNode node, IQueryVisitorContext context)
    {
        _builder.Append(node.ToSqlString());
    }

    public override void Visit(MissingNode node, IQueryVisitorContext context)
    {
        _builder.Append(node.ToSqlString());
    }

    public override async Task<string> AcceptAsync(IQueryNode node, IQueryVisitorContext context)
    {
        await node.AcceptAsync(this, context).ConfigureAwait(false);
        return _builder.ToString();
    }

    public static Task<string> RunAsync(IQueryNode node, IQueryVisitorContext context = null)
    {
        return new GenerateSqlVisitor().AcceptAsync(node, context);
    }

    public static string Run(IQueryNode node, IQueryVisitorContext context = null)
    {
        return RunAsync(node, context).GetAwaiter().GetResult();
    }
}
