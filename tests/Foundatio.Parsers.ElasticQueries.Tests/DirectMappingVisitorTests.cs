using System;
using System.Threading;
using System.Threading.Tasks;
using Elastic.Clients.Elasticsearch;
using Elastic.Clients.Elasticsearch.Mapping;
using Foundatio.Parsers.ElasticQueries.Extensions;
using Foundatio.Parsers.ElasticQueries.Visitors;
using Foundatio.Parsers.LuceneQueries;
using Foundatio.Parsers.LuceneQueries.Nodes;
using Foundatio.Parsers.LuceneQueries.Visitors;
using Xunit;

namespace Foundatio.Parsers.ElasticQueries.Tests;

public class DirectMappingVisitorTests
{
    [Theory]
    [InlineData("term", false)]
    [InlineData("term", true)]
    [InlineData("range", true)]
    [InlineData("exists", true)]
    [InlineData("missing", true)]
    [InlineData("group", false)]
    [InlineData("group", true)]
    public async Task NestedVisitor_WithColdMissingField_DoesNotBlockOrRepeatLoad(string nodeType, bool knownParent)
    {
        var firstLoad = new TaskCompletionSource<TypeMapping?>(TaskCreationOptions.RunContinuationsAsynchronously);
        var unexpectedLoad = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var releaseUnexpectedLoad = new TaskCompletionSource<TypeMapping?>(TaskCreationOptions.RunContinuationsAsynchronously);
        var returned = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        int loads = 0;
        using var resolver = ElasticMappingResolver.CreateWithAsyncLoader(_ =>
            Interlocked.Increment(ref loads) == 1 ? firstLoad.Task : UnexpectedLoadAsync());
        Task<TypeMapping?> UnexpectedLoadAsync()
        {
            unexpectedLoad.TrySetResult();
            return releaseUnexpectedLoad.Task;
        }
        var context = new ElasticQueryVisitorContext { MappingResolver = resolver, QueryType = QueryTypes.Query };
        IQueryNode node = nodeType switch
        {
            "range" => new TermRangeNode { Field = "items.missing", Min = "1", Max = "2" },
            "exists" => new ExistsNode { Field = "items.missing" },
            "missing" => new MissingNode { Field = "items.missing" },
            "group" => new GroupNode
            {
                Field = "items.missing",
                HasParens = true,
                Left = new GroupNode { Field = "items.missing", HasParens = true, Left = new TermNode { Field = "items.missing", Term = "value" } }
            },
            _ => new TermNode { Field = "items.missing", Term = "value" }
        };
        var visitor = new NestedVisitor();
        var operation = Task.Run(async () =>
        {
            var visit = visitor.VisitAsync(node, context);
            returned.TrySetResult();
            await visit;
        }, TestContext.Current.CancellationToken);

        try
        {
            await returned.Task.WaitAsync(TimeSpan.FromSeconds(5), TestContext.Current.CancellationToken);
            Assert.False(operation.IsCompleted);
            firstLoad.SetResult(new TypeMapping
            {
                Properties = knownParent ? new Properties { { "items", new NestedProperty { Properties = new Properties() } } } : new Properties()
            });
            var completed = await Task.WhenAny(operation, unexpectedLoad.Task).WaitAsync(TimeSpan.FromSeconds(5), TestContext.Current.CancellationToken);
            Assert.Same(operation, completed);
        }
        finally
        {
            firstLoad.TrySetResult(null);
            releaseUnexpectedLoad.TrySetResult(null);
            await operation.WaitAsync(TimeSpan.FromSeconds(5), TestContext.Current.CancellationToken);
        }

        Assert.Equal(1, loads);
        if (knownParent)
            Assert.NotNull((await node.GetQueryAsync())?.Nested);
        if (node is GroupNode { Left: GroupNode child })
        {
            Assert.Equal(knownParent ? "items" : null, child.GetNestedPath());
            Assert.Null(await child.Left!.GetQueryAsync());
        }
    }

    [Theory]
    [InlineData(false)]
    [InlineData(true)]
    public async Task NestedVisitor_WithReusedContext_ReleasesMappingsAfterReturnOrException(bool throws)
    {
        TypeMapping mapping = new() { Properties = new Properties { { "items", new NestedProperty() } } };
        using var resolver = new ElasticMappingResolver(() => mapping);
        var context = new ElasticQueryVisitorContext { MappingResolver = resolver, QueryType = QueryTypes.Query };
        var visitor = new NestedVisitor((_, _, _, _) => throws ? Task.FromException<Elastic.Clients.Elasticsearch.QueryDsl.Query?>(new InvalidOperationException("filter")) : Task.FromResult<Elastic.Clients.Elasticsearch.QueryDsl.Query?>(null));
        var first = new GroupNode { Field = "items", HasParens = true };
        if (throws)
            await Assert.ThrowsAsync<InvalidOperationException>(() => visitor.VisitAsync(first, context));
        else
            await visitor.VisitAsync(first, context);

        mapping = new TypeMapping { Properties = new Properties { { "items", new TextProperty() } } };
        resolver.RefreshMapping();
        var second = new GroupNode { Field = "items", HasParens = true };
        await visitor.VisitAsync(second, context);

        Assert.Null(second.GetNestedPath());
        Assert.Null(await second.GetQueryAsync());
    }

    [Fact]
    public async Task GetSortFieldsVisitor_WithCustomOverrideAndOrdinaryContext_PreservesDispatch()
    {
        var visitor = new CustomSortVisitor();

        var sorts = await visitor.AcceptAsync(new TermNode { Field = "custom" }, new QueryVisitorContext());

        Assert.Equal("computed", Assert.Single(sorts).Field!.Field!.Name);
        Assert.Equal(1, visitor.Visits);
    }

    [Fact]
    public async Task GetSortFieldsVisitor_WithPrecomputedSort_DoesNotLoadMappings()
    {
        int loads = 0;
        using var resolver = new ElasticMappingResolver(() => { loads++; return null; });
        var node = new TermNode { Field = "custom" };
        var sort = new SortOptions { Field = new FieldSort("computed") };
        node.SetSort(sort);

        var sorts = await GetSortFieldsVisitor.RunAsync(node, new ElasticQueryVisitorContext { MappingResolver = resolver });

        Assert.Same(sort, Assert.Single(sorts));
        Assert.Equal(0, loads);
    }

    private sealed class CustomSortVisitor : GetSortFieldsVisitor
    {
        public int Visits { get; private set; }

        public override void Visit(TermNode node, IQueryVisitorContext context)
        {
            Visits++;
            node.SetSort(new SortOptions { Field = new FieldSort("computed") });
            base.Visit(node, context);
        }
    }
}
