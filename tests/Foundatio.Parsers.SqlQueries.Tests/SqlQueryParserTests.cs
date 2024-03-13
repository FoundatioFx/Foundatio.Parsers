using System;
using System.Collections.Generic;
using System.Linq;
using System.Linq.Dynamic.Core;
using System.Threading.Tasks;
using Foundatio.Parsers.LuceneQueries.Nodes;
using Foundatio.Parsers.LuceneQueries.Visitors;
using Foundatio.Parsers.SqlQueries.Extensions;
using Foundatio.Parsers.SqlQueries.Visitors;
using Foundatio.Xunit;
using Microsoft.EntityFrameworkCore;
using Microsoft.Extensions.Logging;
using Pegasus.Common.Tracing;
using Xunit;
using Xunit.Abstractions;

namespace Foundatio.Parsers.SqlQueries.Tests;

public class SqlQueryParserTests : TestWithLoggingBase {
    public SqlQueryParserTests(ITestOutputHelper output) : base(output) {
        Log.MinimumLevel = LogLevel.Trace;
    }

    [Theory]
    [InlineData("field:[1 TO 5]", "(field >= 1 AND field <= 5)")]
    [InlineData("field:{1 TO 5}", "(field > 1 AND field < 5)")]
    [InlineData("field:[1 TO 5}", "(field >= 1 AND field < 5)")]
    [InlineData("field:>5", "field > 5")]
    [InlineData("field:>=5", "field >= 5")]
    [InlineData("field:<5", "field < 5")]
    [InlineData("field:<=5", "field <= 5")]
    // [InlineData("date:>now")]
    // [InlineData("date:<now")]
    // [InlineData("_exists_:title")]
    // [InlineData("_missing_:title")]
    // [InlineData("date:[now/d-4d TO now/d+1d}")]
    // [InlineData("(date:[now/d-4d TO now/d+1d})")]
    // [InlineData("data.date:>now")]
    // [InlineData("data.date:[now/d-4d TO now/d+1d}")]
    // [InlineData("data.date:[2012-01-01 TO 2012-12-31]")]
    // [InlineData("data.date:[* TO 2012-12-31]")]
    // [InlineData("data.date:[2012-01-01 TO *]")]
    // [InlineData("(data.date:[now/d-4d TO now/d+1d})")]
    // [InlineData("count:>1")]
    // [InlineData("count:>=1")]
    // [InlineData("count:[1..5}")]
    // [InlineData(@"count:a\:a")]
    // [InlineData(@"count:a\:a more:stuff")]
    // [InlineData("data.count:[1..5}")]
    // [InlineData("age:[1 TO 2]")]
    // [InlineData("data.Windows-identity:ejsmith")]
    // [InlineData("data.age:[* TO 10]")]
    // [InlineData("hidden:true")]
    public Task ValidQueries(string query, string expected)
    {
        return ParseAndValidateQuery(query, expected, true);
    }

    [Fact]
    public async Task CanGenerateSql() {
        var contextOptions = new DbContextOptionsBuilder<SampleContext>()
            .UseSqlServer("Server=localhost;User Id=sa;Password=P@ssword1;Timeout=5;Initial Catalog=foundatio;Encrypt=False")
            .Options;

        await using var context = new SampleContext(contextOptions);
        await context.Database.EnsureDeletedAsync();
        await context.Database.EnsureCreatedAsync();

        var company = new Company {
            Name = "Acme",
            DataDefinitions = [ new() { Key = "age", DataType = DataType.Number } ]
        };
        context.Companies.Add(company);
        context.Employees.Add(new Employee
        {
            FullName = "John Doe",
            Title = "Software Developer",
            DataValues = [ new() { Definition = company.DataDefinitions[0], NumberValue = 30 } ],
            Company = company
        });
        context.Employees.Add(new Employee
        {
            FullName = "Jane Doe",
            Title = "Software Developer",
            DataValues = [ new() { Definition = company.DataDefinitions[0], NumberValue = 23 } ],
            Company = company
        });
        await context.SaveChangesAsync();

        var parser = new SqlQueryParser();
        parser.Configuration.UseFieldMap(new Dictionary<string, string> {{ "age", "DataValues.Any(DataDefinitionId = 1 AND NumberValue" }});

        string sqlExpected = context.Employees.Where(e => e.Company.Name == "acme" && e.DataValues.Any(dv => dv.DataDefinitionId == 1 && dv.NumberValue == 30)).ToQueryString();
        string sqlActual = context.Employees.Where("""company.name = "acme" AND DataValues.Any(DataDefinitionId = 1 AND NumberValue = 30) """).ToQueryString();
        Assert.Equal(sqlExpected, sqlActual);
        sqlActual = context.Employees.LuceneWhere("company.name:acme (age:1 OR age:>30)").ToQueryString();
        Assert.Equal(sqlExpected, sqlActual);

        var employees = await context.Employees.Where(e => e.Title == "software developer" && e.DataValues.Any(dv => dv.DataDefinitionId == 1 && dv.NumberValue == 30))
            .ToListAsync();

        Assert.Single(employees);
        var employee = employees.Single();
        Assert.Equal("John Doe", employee.FullName);
    }

    private async Task ParseAndValidateQuery(string query, string expected, bool isValid)
    {
#if ENABLE_TRACING
        var tracer = new LoggingTracer(_logger, reportPerformance: true);
#else
        var tracer = NullTracer.Instance;
#endif
        var parser = new SqlQueryParser
        {
            Tracer = tracer
        };

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

        string nodes = await DebugQueryVisitor.RunAsync(result);
        _logger.LogInformation(nodes);
        string generatedQuery = await GenerateSqlVisitor.RunAsync(result);
        Assert.Equal(expected, generatedQuery);
    }
}
