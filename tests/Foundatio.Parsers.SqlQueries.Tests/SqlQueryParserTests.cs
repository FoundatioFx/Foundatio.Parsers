using System;
using System.Collections.Generic;
using System.Threading.Tasks;
using Foundatio.Parsers.LuceneQueries.Nodes;
using Foundatio.Parsers.SqlQueries.Visitors;
using Foundatio.Xunit;
using Microsoft.EntityFrameworkCore;
using Microsoft.Extensions.Logging;
using Xunit;
using Xunit.Abstractions;

namespace Foundatio.Parsers.SqlQueries.Tests;

public class SqlQueryParserTests : TestWithLoggingBase {
    public SqlQueryParserTests(ITestOutputHelper output) : base(output) {
        Log.MinimumLevel = LogLevel.Trace;
    }

    [Theory]
    [InlineData("value1 value2", GroupOperator.Default, "value1 value2")]
    [InlineData("value1 value2", GroupOperator.And, "value1 AND value2")]
    [InlineData("value1 value2", GroupOperator.Or, "value1 OR value2")]
    [InlineData("value1 value2 value3", GroupOperator.Default, "value1 value2 value3")]
    [InlineData("value1 value2 value3", GroupOperator.And, "value1 AND value2 AND value3")]
    [InlineData("value1 value2 value3", GroupOperator.Or, "value1 OR value2 OR value3")]
    [InlineData("value1 value2 value3 value4", GroupOperator.And, "value1 AND value2 AND value3 AND value4")]
    [InlineData("(value1 value2) OR (value3 value4)", GroupOperator.And, "(value1 AND value2) OR (value3 AND value4)")]
    public async Task DefaultOperatorApplied(string query, GroupOperator groupOperator, string expected) {
        var contextOptions = new DbContextOptionsBuilder<SampleContext>()
            .UseInMemoryDatabase(Guid.NewGuid().ToString())
            .Options;
        await using var context = new SampleContext(contextOptions);
        var company = new Company { Name = "Acme" };
        context.Companies.Add(company);
        context.Employees.Add(new Employee { FullName = "John Doe", Title = "Software Developer", Company = company });
        context.Employees.Add(new Employee { FullName = "Jane Doe", Title = "Software Developer", Company = company });
        await context.SaveChangesAsync();
        
        var parser = new SqlQueryParser(config => config.SetDefaultFields(["FullName", "Title"]));
        var result = await parser.ParseAsync(query, new SqlQueryVisitorContext { DefaultOperator = groupOperator });
        Assert.NotNull(result);
        Assert.Equal(expected, result.ToString());
    }
}

public class SampleContext : DbContext {
    public SampleContext(DbContextOptions<SampleContext> options) : base(options) { }
    public DbSet<Employee> Employees { get; set; }
    public DbSet<Company> Companies { get; set; }
    
    protected override void OnConfiguring(DbContextOptionsBuilder optionsBuilder) {
        base.OnConfiguring(optionsBuilder);
    }
}

public class Employee {
    public int Id { get; set; }
    public string FullName { get; set; }
    public string Title { get; set; }
    public int CompanyId { get; set; }
    public Company Company { get; set; }
}

public class Company {
    public int Id { get; set; }
    public string Name { get; set; }
    public string Description { get; set; }
    public List<Employee> Employees { get; set; }
}
