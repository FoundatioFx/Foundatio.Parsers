using System;
using System.Collections.Generic;
using System.ComponentModel.DataAnnotations;
using System.Linq.Dynamic.Core;
using System.Threading.Tasks;
using Foundatio.Parsers.SqlQueries.Visitors;
using Foundatio.Xunit;
using Microsoft.EntityFrameworkCore;
using Microsoft.Extensions.Logging;
using Xunit;

namespace Foundatio.Parsers.SqlQueries.Tests;

public class SqlQuerySyntaxTests : TestWithLoggingBase
{
    public SqlQuerySyntaxTests(ITestOutputHelper output) : base(output)
    {
        Log.DefaultLogLevel = LogLevel.Trace;
    }

    [Theory]
    [InlineData("FullName:jo?n", "jo_n")]
    [InlineData("FullName:jo*n", "jo%n")]
    [InlineData("FullName:*ohn", "%ohn")]
    [InlineData("FullName:jo?n*", "jo_n%")]
    public async Task ToDynamicLinqAsync_WithAdvancedWildcard_TranslatesToSqlLike(string query, string expectedPattern)
    {
        // Arrange
        using var db = CreateContext();
        var parser = new SqlQueryParser(configuration => configuration.SetLoggerFactory(Log));

        // Act
        string predicate = await parser.ToDynamicLinqAsync(query, parser.GetContext(db.Employees.EntityType));
        string sql = db.Employees.Where(parser.ParsingConfig, predicate).ToQueryString();

        // Assert
        Assert.Contains(" LIKE ", sql, StringComparison.OrdinalIgnoreCase);
        Assert.Contains(expectedPattern, sql, StringComparison.Ordinal);
        Assert.Contains(" ESCAPE ", sql, StringComparison.OrdinalIgnoreCase);
    }

    [Fact]
    public async Task ToDynamicLinqAsync_WithAliasedInclude_TranslatesAdvancedWildcard()
    {
        // Arrange
        using var db = CreateContext();
        var parser = new SqlQueryParser(c => c.SetLoggerFactory(Log)
            .UseFieldMap(new Dictionary<string, string> { ["name"] = "FullName" })
            .UseIncludes(new Dictionary<string, string> { ["find"] = "name:jo?n" }));

        var context = parser.GetContext(db.Employees.EntityType);
        context.ValidationOptions!.AllowedFields.Add("name");

        // Act
        string predicate = await parser.ToDynamicLinqAsync("@include:find", context);
        string sql = db.Employees.Where(parser.ParsingConfig, predicate).ToQueryString();

        // Assert
        Assert.Contains(" LIKE ", sql, StringComparison.OrdinalIgnoreCase);
        Assert.Contains("jo_n", sql, StringComparison.Ordinal);
    }

    [Fact]
    public async Task ToDynamicLinqAsync_WithDefaultAndNavigationFields_TranslatesAdvancedWildcard()
    {
        // Arrange
        using var db = CreateContext();
        var parser = new SqlQueryParser(c => c.SetLoggerFactory(Log).SetDefaultFields(["FullName", "Title"]));

        // Act
        string defaultPredicate = await parser.ToDynamicLinqAsync("jo?n", parser.GetContext(db.Employees.EntityType));
        string defaultSql = db.Employees.Where(parser.ParsingConfig, defaultPredicate).ToQueryString();
        string navigationPredicate = await parser.ToDynamicLinqAsync("CurrentCompany.Name:jo?n", parser.GetContext(db.Employees.EntityType));
        string navigationSql = db.Employees.Where(parser.ParsingConfig, navigationPredicate).ToQueryString();

        // Assert
        Assert.Equal(2, defaultSql.Split(" LIKE ").Length - 1);
        Assert.Contains("jo_n", defaultSql, StringComparison.Ordinal);
        Assert.Contains(" LIKE ", navigationSql, StringComparison.OrdinalIgnoreCase);
        Assert.Contains("jo_n", navigationSql, StringComparison.Ordinal);
    }

    [Fact]
    public async Task ToDynamicLinqAsync_WithExcludedDefaultFields_TranslatesNegatedWildcard()
    {
        // Arrange
        using var db = CreateContext();
        var parser = new SqlQueryParser(configuration => configuration
            .SetLoggerFactory(Log)
            .SetDefaultFields(["FullName", "Title"]));

        // Act
        string predicate = await parser.ToDynamicLinqAsync("-jo?n", parser.GetContext(db.Employees.EntityType));
        string sql = db.Employees.Where(parser.ParsingConfig, predicate).ToQueryString();

        // Assert
        Assert.StartsWith("!(", predicate);
        Assert.DoesNotContain("!((", predicate, StringComparison.Ordinal);
        Assert.Equal(2, sql.Split(" LIKE ").Length - 1);
        Assert.Contains("NOT", sql, StringComparison.OrdinalIgnoreCase);
    }

    [Theory]
    [InlineData(@"jo\*n", "jo*n")]
    [InlineData(@"jo\?n", "jo?n")]
    public async Task ToDynamicLinqAsync_WithEscapedDefaultFieldWildcard_PreservesLiteralCharacter(string query, string expectedTerm)
    {
        // Arrange
        using var db = CreateContext();
        var parser = new SqlQueryParser(configuration => configuration
            .SetLoggerFactory(Log)
            .SetDefaultFields(["FullName"]));

        // Act
        string predicate = await parser.ToDynamicLinqAsync(query, parser.GetContext(db.Employees.EntityType));

        // Assert
        Assert.Equal($"((FullName.StartsWith(\"{expectedTerm}\")))", predicate);
    }

    [Theory]
    [InlineData(@"FullName:jo\*n", "jo*n")]
    [InlineData(@"FullName:jo\?n", "jo?n")]
    [InlineData("FullName:\"jo*n\"", "jo*n")]
    public async Task ToDynamicLinqAsync_WithEscapedOrQuotedWildcard_KeepsLiteralValue(string query, string expectedValue)
    {
        // Arrange
        using var db = CreateContext();
        var parser = new SqlQueryParser(configuration => configuration.SetLoggerFactory(Log));

        // Act
        string predicate = await parser.ToDynamicLinqAsync(query, parser.GetContext(db.Employees.EntityType));
        string sql = db.Employees.Where(parser.ParsingConfig, predicate).ToQueryString();

        // Assert
        Assert.DoesNotContain(" LIKE ", sql, StringComparison.OrdinalIgnoreCase);
        Assert.Contains(expectedValue, sql, StringComparison.Ordinal);
    }

    [Fact]
    public async Task ToDynamicLinqAsync_WithManualFieldMetadata_PreservesPrefixSearch()
    {
        // Arrange
        var parser = new SqlQueryParser(configuration => configuration.SetLoggerFactory(Log));
        var context = new SqlQueryVisitorContext
        {
            Fields = [new EntityFieldInfo { Name = "FullName", FullName = "name" }]
        };

        // Act
        string predicate = await parser.ToDynamicLinqAsync("name:Jo*", context);

        // Assert
        Assert.Equal("FullName.StartsWith(\"Jo\")", predicate);
    }

    [Fact]
    public async Task ToDynamicLinqAsync_WithSqlLikeCharacters_EscapesLiteralCharacters()
    {
        // Arrange
        using var db = CreateContext();
        var parser = new SqlQueryParser(configuration => configuration.SetLoggerFactory(Log));

        // Act
        string predicate = await parser.ToDynamicLinqAsync("FullName:jo%_?", parser.GetContext(db.Employees.EntityType));
        string sql = db.Employees.Where(parser.ParsingConfig, predicate).ToQueryString();

        // Assert
        Assert.Contains("LIKE", sql, StringComparison.OrdinalIgnoreCase);
        Assert.Contains(@"jo\%\__", sql, StringComparison.Ordinal);
    }

    [Fact]
    public async Task ValidateAsync_WithAdvancedWildcardOnFullTextField_ReturnsError()
    {
        // Arrange
        using var db = CreateContext();
        var parser = new SqlQueryParser(c => c.SetLoggerFactory(Log).SetFullTextFields(["FullName"]));

        // Act
        var result = await parser.ValidateAsync("FullName:jo?n", parser.GetContext(db.Employees.EntityType));

        // Assert
        Assert.False(result.IsValid);
        Assert.Contains("wildcard", result.Message, StringComparison.OrdinalIgnoreCase);
    }

    [Fact]
    public async Task ValidateAsync_WithAdvancedWildcardWithoutMappedType_ReturnsError()
    {
        // Arrange
        var parser = new SqlQueryParser(configuration => configuration.SetLoggerFactory(Log));
        var context = new SqlQueryVisitorContext
        {
            Fields = [new EntityFieldInfo { Name = "FullName", FullName = "name" }]
        };

        // Act
        var validation = await parser.ValidateAsync("name:Jo?n", context);

        // Assert
        Assert.False(validation.IsValid);
        Assert.Contains("mapped string field", validation.Message, StringComparison.OrdinalIgnoreCase);
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
    public async Task ValidateAsync_WithUnsupportedSqlSyntax_ReturnsErrorBeforeGeneration(string query, string kind)
    {
        // Arrange
        using var db = CreateContext();
        var parser = new SqlQueryParser(configuration => configuration.SetLoggerFactory(Log));

        // Act
        var result = await parser.ValidateAsync(query, parser.GetContext(db.Employees.EntityType));
        var error = await Assert.ThrowsAsync<ValidationException>(() =>
            parser.ToDynamicLinqAsync(query, parser.GetContext(db.Employees.EntityType)));

        // Assert
        Assert.False(result.IsValid);
        Assert.Contains(kind, result.Message, StringComparison.OrdinalIgnoreCase);
        Assert.Contains(kind, error.Message, StringComparison.OrdinalIgnoreCase);
    }

    private static SampleContext CreateContext() => new(new DbContextOptionsBuilder<SampleContext>()
        .UseSqlServer()
        .Options);
}
