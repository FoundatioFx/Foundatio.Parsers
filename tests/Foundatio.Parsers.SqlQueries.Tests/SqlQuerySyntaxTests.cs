using System;
using System.Collections.Generic;
using System.ComponentModel.DataAnnotations;
using System.Linq.Dynamic.Core;
using System.Threading.Tasks;
using Foundatio.Parsers.SqlQueries.Visitors;
using Microsoft.EntityFrameworkCore;
using Xunit;

namespace Foundatio.Parsers.SqlQueries.Tests;

public class SqlQuerySyntaxTests
{
    [Theory]
    [InlineData("FullName:jo?n", "jo_n")]
    [InlineData("FullName:jo*n", "jo%n")]
    [InlineData("FullName:*ohn", "%ohn")]
    [InlineData("FullName:jo?n*", "jo_n%")]
    public async Task AdvancedWildcards_TranslateToSqlLike(string query, string expectedPattern)
    {
        using var db = CreateContext();
        var parser = new SqlQueryParser();

        string predicate = await parser.ToDynamicLinqAsync(query, parser.GetContext(db.Employees.EntityType));
        string sql = db.Employees.Where(parser.ParsingConfig, predicate).ToQueryString();

        Assert.Contains(" LIKE ", sql, StringComparison.OrdinalIgnoreCase);
        Assert.Contains(expectedPattern, sql, StringComparison.Ordinal);
        Assert.Contains(" ESCAPE ", sql, StringComparison.OrdinalIgnoreCase);
    }

    [Theory]
    [InlineData(@"FullName:jo\*n", "jo*n")]
    [InlineData(@"FullName:jo\?n", "jo?n")]
    [InlineData("FullName:\"jo*n\"", "jo*n")]
    public async Task EscapedAndQuotedWildcards_RemainLiteral(string query, string expectedValue)
    {
        using var db = CreateContext();
        var parser = new SqlQueryParser();

        string predicate = await parser.ToDynamicLinqAsync(query, parser.GetContext(db.Employees.EntityType));
        string sql = db.Employees.Where(parser.ParsingConfig, predicate).ToQueryString();

        Assert.DoesNotContain(" LIKE ", sql, StringComparison.OrdinalIgnoreCase);
        Assert.Contains(expectedValue, sql, StringComparison.Ordinal);
    }

    [Fact]
    public async Task AdvancedWildcard_EscapesSqlPatternCharacters()
    {
        using var db = CreateContext();
        var parser = new SqlQueryParser();

        string predicate = await parser.ToDynamicLinqAsync("FullName:jo%_?", parser.GetContext(db.Employees.EntityType));
        string sql = db.Employees.Where(parser.ParsingConfig, predicate).ToQueryString();

        Assert.Contains("LIKE", sql, StringComparison.OrdinalIgnoreCase);
        Assert.Contains(@"jo\%\__", sql, StringComparison.Ordinal);
    }

    [Fact]
    public async Task AdvancedWildcard_OnDefaultAndNavigationFields_Translates()
    {
        using var db = CreateContext();
        var parser = new SqlQueryParser(c => c.SetDefaultFields(["FullName", "Title"]));

        string defaultPredicate = await parser.ToDynamicLinqAsync("jo?n", parser.GetContext(db.Employees.EntityType));
        string defaultSql = db.Employees.Where(parser.ParsingConfig, defaultPredicate).ToQueryString();
        string navigationPredicate = await parser.ToDynamicLinqAsync("CurrentCompany.Name:jo?n", parser.GetContext(db.Employees.EntityType));
        string navigationSql = db.Employees.Where(parser.ParsingConfig, navigationPredicate).ToQueryString();

        Assert.Equal(2, defaultSql.Split(" LIKE ").Length - 1);
        Assert.Contains("jo_n", defaultSql, StringComparison.Ordinal);
        Assert.Contains(" LIKE ", navigationSql, StringComparison.OrdinalIgnoreCase);
        Assert.Contains("jo_n", navigationSql, StringComparison.Ordinal);
    }

    [Theory]
    [InlineData("FullName:/jo.*/", "regex")]
    [InlineData("FullName:john~1", "fuzzy")]
    [InlineData("FullName:\"John Smith\"~2", "proximity")]
    [InlineData("FullName:john^2", "boost")]
    [InlineData("(FullName:john)^2", "boost")]
    [InlineData("Salary:[1 TO 5]^2", "boost")]
    [InlineData("Salary:1?0", "wildcard")]
    [InlineData("Salary:1*", "wildcard")]
    public async Task UnsupportedSqlSyntax_IsRejectedBeforeGeneratingPredicate(string query, string kind)
    {
        using var db = CreateContext();
        var parser = new SqlQueryParser();

        var result = await parser.ValidateAsync(query, parser.GetContext(db.Employees.EntityType));
        var error = await Assert.ThrowsAsync<ValidationException>(() =>
            parser.ToDynamicLinqAsync(query, parser.GetContext(db.Employees.EntityType)));

        Assert.False(result.IsValid);
        Assert.Contains(kind, result.Message, StringComparison.OrdinalIgnoreCase);
        Assert.Contains(kind, error.Message, StringComparison.OrdinalIgnoreCase);
    }

    [Fact]
    public async Task AdvancedWildcard_OnFullTextField_IsRejected()
    {
        using var db = CreateContext();
        var parser = new SqlQueryParser(c => c.SetFullTextFields(["FullName"]));

        var result = await parser.ValidateAsync("FullName:jo?n", parser.GetContext(db.Employees.EntityType));

        Assert.False(result.IsValid);
        Assert.Contains("wildcard", result.Message, StringComparison.OrdinalIgnoreCase);
    }

    [Fact]
    public async Task AdvancedWildcard_ThroughAliasAndInclude_Translates()
    {
        using var db = CreateContext();
        var parser = new SqlQueryParser(c => c
            .UseFieldMap(new Dictionary<string, string> { ["name"] = "FullName" })
            .UseIncludes(new Dictionary<string, string> { ["find"] = "name:jo?n" }));

        var context = parser.GetContext(db.Employees.EntityType);
        context.ValidationOptions!.AllowedFields.Add("name");
        string predicate = await parser.ToDynamicLinqAsync("@include:find", context);
        string sql = db.Employees.Where(parser.ParsingConfig, predicate).ToQueryString();

        Assert.Contains(" LIKE ", sql, StringComparison.OrdinalIgnoreCase);
        Assert.Contains("jo_n", sql, StringComparison.Ordinal);
    }

    private static SampleContext CreateContext() => new(new DbContextOptionsBuilder<SampleContext>()
        .UseSqlServer("Server=localhost;Database=foundatio;User Id=sa;Password=P@ssword1;Encrypt=False")
        .Options);
}
