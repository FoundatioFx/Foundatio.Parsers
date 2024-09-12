using System;
using System.Collections.Generic;
using System.Threading.Tasks;
using Elastic.Clients.Elasticsearch;
using Foundatio.Parsers.LuceneQueries;
using Foundatio.Parsers.LuceneQueries.Extensions;
using Foundatio.Parsers.LuceneQueries.Nodes;
using Foundatio.Parsers.LuceneQueries.Visitors;
using Microsoft.Extensions.Logging;
using Xunit;
using Xunit.Abstractions;

namespace Foundatio.Parsers.ElasticQueries.Tests;

public class InvertQueryTests : ElasticsearchTestBase<SampleDataFixture>
{
    public const string OrgId = "1";
    public const string AltOrgId = "2";

    public InvertQueryTests(ITestOutputHelper output, SampleDataFixture fixture) : base(output, fixture) { }

    [Fact]
    public Task CanInvertTermQuery()
    {
        return InvertAndValidateQuery("deleted", "(NOT deleted)", null, true);
    }

    [Fact]
    public Task CanInvertFieldQuery()
    {
        return InvertAndValidateQuery("status:open", "(NOT status:open)", null, true);
    }

    [Fact]
    public Task CanInvertNotFieldQuery()
    {
        return InvertAndValidateQuery("NOT status:open", "status:open", null, true);
    }

    [Fact]
    public Task CanInvertMultipleTermsQuery()
    {
        return InvertAndValidateQuery("field1:value field2:value field3:value", "(NOT (field1:value field2:value field3:value))", null, true);
    }

    [Fact]
    public Task CanInvertOrGroupQuery()
    {
        return InvertAndValidateQuery("(field1:value OR field2:value)", "(NOT (field1:value OR field2:value))", null, true);
    }

    [Fact]
    public Task CanInvertFieldWithNonInvertedFieldQuery()
    {
        return InvertAndValidateQuery("field:value organizationId:value", "(NOT field:value) organizationId:value", null, true);
    }

    [Fact]
    public Task CanInvertAlternateCriteria()
    {
        return InvertAndValidateQuery("value", "(is_deleted:true OR (NOT value))", "is_deleted:true", true);
    }

    [Fact]
    public Task CanInvertAlternateCriteriaAndNonInvertedField()
    {
        return InvertAndValidateQuery("organizationId:value field1:value", "organizationId:value (is_deleted:true OR (NOT field1:value))", "is_deleted:true", true);
    }

    [Fact]
    public Task CanInvertNonInvertedFieldAndOrGroup()
    {
        return InvertAndValidateQuery("organizationId:value (field1:value OR field2:value)", "organizationId:value (NOT (field1:value OR field2:value))", null, true);
    }

    [Fact]
    public Task CanInvertAlternateCriteriaAndNonInvertedFieldAndOrGroup()
    {
        return InvertAndValidateQuery("organizationId:value (field1:value OR field2:value)", "organizationId:value (is_deleted:true OR (NOT (field1:value OR field2:value)))", "is_deleted:true", true);
    }

    [Fact]
    public Task CanInvertGroupNonInvertedField()
    {
        return InvertAndValidateQuery("(field1:value organizationId:value) field2:value", "((NOT field1:value) organizationId:value) (NOT field2:value)", null, true);
    }

    private async Task InvertAndValidateQuery(string query, string expected, string alternateInvertedCriteria, bool isValid)
    {
        var parser = new LuceneQueryParser();

        IQueryNode result;
        try
        {
            result = await parser.ParseAsync(query);
        }
        catch (FormatException ex)
        {
            Assert.False(isValid, ex.Message);
            return;
        }

        var invertQueryVisitor = new InvertQueryVisitor(["organizationId"]);
        var context = new QueryVisitorContext();

        if (!String.IsNullOrWhiteSpace(alternateInvertedCriteria))
        {
            var invertedAlternate = await parser.ParseAsync(alternateInvertedCriteria);
            context.SetAlternateInvertedCriteria(invertedAlternate);
        }

        result = await invertQueryVisitor.AcceptAsync(result, context);
        string invertedQuery = result.ToString();
        string nodes = await DebugQueryVisitor.RunAsync(result);
        _logger.LogInformation("{Result}", nodes);
        Assert.Equal(expected, invertedQuery);

        var total = await Client.CountAsync<InvertTest>();
        var results = await Client.SearchAsync<InvertTest>(s => s.QueryOnQueryString(query).TrackTotalHits(true));
        var invertedResults = await Client.SearchAsync<InvertTest>(s => s.QueryOnQueryString(invertedQuery).TrackTotalHits(true));

        Assert.Equal(total.Count, results.Total + invertedResults.Total);
    }
}

public class InvertTest
{
    public const string OrgId = "1";
    public const string AltOrgId = "2";

    public string Id { get; set; }
    public string OrganizationId { get; set; }
    public string Description { get; set; }
    public string Status { get; set; } = "open";
    public bool IsDeleted { get; set; }
}

public class SampleDataFixture : ElasticsearchFixture
{
    public override async Task InitializeAsync()
    {
        await base.InitializeAsync();

        const string indexName = "test_invert";
        CreateNamedIndex<InvertTest>(indexName, m => m
            .Properties(p => p
                .Keyword(p1 => p1.Name(n => n.Id))
                .Keyword(p1 => p1.Name(n => n.OrganizationId))
                .Text(p1 => p1.Name(n => n.Description))
                .Keyword(p1 => p1.Name(n => n.Status))
                .Boolean(p1 => p1.Name(n => n.IsDeleted))
            ));

        var records = new List<InvertTest>();
        int id = 1;

        for (int i = 0; i < 10000; i++, id++)
        {
            records.Add(new InvertTest { Id = id.ToString(), OrganizationId = InvertTest.OrgId, Description = $"Description {i}", Status = "open", IsDeleted = false });
        }
        for (int i = 0; i < 1000; i++, id++)
        {
            records.Add(new InvertTest { Id = id.ToString(), OrganizationId = InvertTest.OrgId, Description = $"Deleted Description {i}", Status = "open", IsDeleted = true });
        }
        for (int i = 0; i < 100; i++, id++)
        {
            records.Add(new InvertTest { Id = id.ToString(), OrganizationId = InvertTest.OrgId, Description = $"Regressed Description {i}", Status = "regressed", IsDeleted = false });
        }
        for (int i = 0; i < 100; i++, id++)
        {
            records.Add(new InvertTest { Id = id.ToString(), OrganizationId = InvertTest.OrgId, Description = $"Ignored Description {i}", Status = "ignored", IsDeleted = false });
        }

        for (int i = 0; i < 10000; i++, id++)
        {
            records.Add(new InvertTest { Id = id.ToString(), OrganizationId = InvertTest.AltOrgId, Description = $"Alt Description {i}", Status = "open", IsDeleted = false });
        }
        for (int i = 0; i < 1000; i++, id++)
        {
            records.Add(new InvertTest { Id = id.ToString(), OrganizationId = InvertTest.AltOrgId, Description = $"Deleted Alt Description {i}", Status = "open", IsDeleted = true });
        }
        for (int i = 0; i < 100; i++, id++)
        {
            records.Add(new InvertTest { Id = id.ToString(), OrganizationId = InvertTest.AltOrgId, Description = $"Regressed Alt Description {i}", Status = "regressed", IsDeleted = false });
        }
        for (int i = 0; i < 100; i++, id++)
        {
            records.Add(new InvertTest { Id = id.ToString(), OrganizationId = InvertTest.AltOrgId, Description = $"Ignored Alt Description {i}", Status = "ignored", IsDeleted = false });
        }

        await Client.IndexManyAsync(records, indexName);
        await Client.Indices.RefreshAsync(indexName);
    }
}
